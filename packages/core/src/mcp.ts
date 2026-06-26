export * as MCP from "./mcp"

import { Context, Effect, Exit, Layer, Scope, Schema, Semaphore } from "effect"
import { Config } from "./config"
import { ConfigMCP } from "./config/mcp"

export const ServerConfig = ConfigMCP.Server
export type ServerConfig = typeof ServerConfig.Type

export class Timeout extends Schema.Class<Timeout>("MCP.Timeout")({
  startup: Schema.Number,
  request: Schema.Number,
}) {}

export class Disconnected extends Schema.Class<Disconnected>("MCP.Status.Disconnected")({
  status: Schema.Literal("disconnected"),
}) {}

export class Connecting extends Schema.Class<Connecting>("MCP.Status.Connecting")({
  status: Schema.Literal("connecting"),
}) {}

export class Connected extends Schema.Class<Connected>("MCP.Status.Connected")({
  status: Schema.Literal("connected"),
}) {}

export class Disabled extends Schema.Class<Disabled>("MCP.Status.Disabled")({
  status: Schema.Literal("disabled"),
}) {}

export class NeedsAuth extends Schema.Class<NeedsAuth>("MCP.Status.NeedsAuth")({
  status: Schema.Literal("needs_auth"),
}) {}

export class Failed extends Schema.Class<Failed>("MCP.Status.Failed")({
  status: Schema.Literal("failed"),
  message: Schema.String,
}) {}

export const Status = Schema.Union([Disconnected, Connecting, Connected, Disabled, NeedsAuth, Failed]).pipe(
  Schema.toTaggedUnion("status"),
)
export type Status = typeof Status.Type

export class Server extends Schema.Class<Server>("MCP.Server")({
  name: Schema.String,
  config: ServerConfig,
  timeout: Timeout,
  status: Status,
}) {}

export class NotFoundError extends Schema.TaggedErrorClass<NotFoundError>()("MCP.NotFoundError", {
  name: Schema.String,
}) {}

export class DisabledError extends Schema.TaggedErrorClass<DisabledError>()("MCP.DisabledError", {
  name: Schema.String,
}) {}

export class ConnectionError extends Schema.TaggedErrorClass<ConnectionError>()("MCP.ConnectionError", {
  name: Schema.String,
  message: Schema.String,
}) {}

export type Client = object

export interface ConnectInput {
  readonly name: string
  readonly config: ServerConfig
  readonly timeout: Timeout
}

export interface ConnectorInterface {
  /** Connects one configured server and registers all transport cleanup in the provided Scope. */
  readonly connect: (input: ConnectInput) => Effect.Effect<Client, ConnectionError, Scope.Scope>
}

export class Connector extends Context.Service<Connector, ConnectorInterface>()("@opencode/v2/MCP/Connector") {}

export interface Interface {
  readonly get: (name: string) => Effect.Effect<Server | undefined>
  readonly list: () => Effect.Effect<Server[]>
  readonly connect: (name: string) => Effect.Effect<void, NotFoundError | DisabledError | ConnectionError>
  readonly disconnect: (name: string) => Effect.Effect<void, NotFoundError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/MCP") {}

type Entry = {
  config: ServerConfig
  timeout: Timeout
  status: Status
  scope?: Scope.Closeable
  client?: Client
}

const DEFAULT_STARTUP_TIMEOUT = 30_000
const DEFAULT_REQUEST_TIMEOUT = 300_000

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const config = yield* Config.Service
    const connector = yield* Connector
    const semaphore = Semaphore.makeUnsafe(1)
    const entries = new Map<string, Entry>()
    const configured = yield* loadConfig(config)

    for (const [name, server] of configured.servers) {
      entries.set(name, {
        config: server,
        timeout: new Timeout({
          startup: server.timeout?.startup ?? configured.timeout.startup,
          request: server.timeout?.request ?? configured.timeout.request,
        }),
        status: server.disabled ? new Disabled({ status: "disabled" }) : new Disconnected({ status: "disconnected" }),
      })
    }

    const close = (entry: Entry) => {
      const scope = entry.scope
      entry.scope = undefined
      entry.client = undefined
      return scope ? Scope.close(scope, Exit.void) : Effect.void
    }

    yield* Effect.addFinalizer(() =>
      Effect.forEach(entries.values(), close, { discard: true }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            entries.clear()
          }),
        ),
      ),
    )

    const project = (name: string, entry: Entry) =>
      new Server({ name, config: entry.config, timeout: entry.timeout, status: entry.status })

    const requireEntry = (name: string) => {
      const entry = entries.get(name)
      return entry ? Effect.succeed(entry) : Effect.fail(new NotFoundError({ name }))
    }

    return Service.of({
      get: Effect.fn("MCP.get")(function* (name) {
        const entry = entries.get(name)
        return entry && project(name, entry)
      }),
      list: Effect.fn("MCP.list")(function* () {
        return Array.from(entries, ([name, entry]) => project(name, entry))
      }),
      connect: Effect.fn("MCP.connect")(function* (name) {
        const entry = yield* requireEntry(name)
        if (entry.config.disabled) return yield* new DisabledError({ name })
        yield* semaphore.withPermit(
          Effect.gen(function* () {
            yield* close(entry)
            entry.status = new Connecting({ status: "connecting" })
            const scope = yield* Scope.make()
            const client = yield* connector.connect({ name, config: entry.config, timeout: entry.timeout }).pipe(
              Effect.provideService(Scope.Scope, scope),
              Effect.tapError((error) =>
                Effect.sync(() => {
                  entry.status = new Failed({ status: "failed", message: error.message })
                }),
              ),
              Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void)),
            )
            entry.scope = scope
            entry.client = client
            entry.status = new Connected({ status: "connected" })
          }),
        )
      }),
      disconnect: Effect.fn("MCP.disconnect")(function* (name) {
        const entry = yield* requireEntry(name)
        yield* semaphore.withPermit(
          close(entry).pipe(
            Effect.andThen(
              Effect.sync(() => {
                entry.status = entry.config.disabled
                  ? new Disabled({ status: "disabled" })
                  : new Disconnected({ status: "disconnected" })
              }),
            ),
          ),
        )
      }),
    })
  }),
)

export const unimplementedConnectorLayer = Layer.succeed(
  Connector,
  Connector.of({
    connect: (input) =>
      Effect.fail(
        new ConnectionError({
          name: input.name,
          message: "MCP connector is not implemented",
        }),
      ),
  }),
)

export const locationLayer = layer.pipe(Layer.provide(unimplementedConnectorLayer), Layer.provide(Config.locationLayer))

function loadConfig(config: Config.Interface) {
  return Effect.gen(function* () {
    const timeout = { startup: DEFAULT_STARTUP_TIMEOUT, request: DEFAULT_REQUEST_TIMEOUT }
    const servers = new Map<string, ServerConfig>()
    for (const entry of yield* config.entries()) {
      if (entry.type !== "document" || !entry.info.mcp) continue
      if (entry.info.mcp.timeout?.startup !== undefined) timeout.startup = entry.info.mcp.timeout.startup
      if (entry.info.mcp.timeout?.request !== undefined) timeout.request = entry.info.mcp.timeout.request
      for (const [name, server] of Object.entries(entry.info.mcp.servers ?? {})) servers.set(name, server)
    }
    return { timeout, servers }
  })
}
