import { describe, expect } from "bun:test"
import { Config } from "@opencode-ai/core/config"
import { MCP } from "@opencode-ai/core/mcp"
import { Context, Effect, Exit, Layer, Schema, Scope } from "effect"
import { testEffect } from "./lib/effect"

const decode = Schema.decodeUnknownSync(Config.Info)

function config(...values: unknown[]) {
  return Layer.succeed(
    Config.Service,
    Config.Service.of({
      entries: () =>
        Effect.succeed(
          values.map(
            (value, index) =>
              new Config.Document({ type: "document", path: `/config/${index}.json`, info: decode(value) }),
          ),
        ),
    }),
  )
}

function connector(closed: string[]) {
  return Layer.succeed(
    MCP.Connector,
    MCP.Connector.of({
      connect: (input) =>
        Effect.gen(function* () {
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              closed.push(input.name)
            }),
          )
          return { name: input.name }
        }),
    }),
  )
}

function layer(closed: string[] = []) {
  return MCP.layer.pipe(
    Layer.provide(connector(closed)),
    Layer.provide(
      config(
        {
          mcp: {
            timeout: { startup: 10_000, request: 60_000 },
            servers: {
              local: { type: "local", command: ["node", "server.js"] },
              disabled: { type: "remote", url: "https://example.com/mcp", disabled: true },
            },
          },
        },
        {
          mcp: {
            timeout: { request: 120_000 },
            servers: {
              local: { type: "local", command: ["bun", "server.ts"], timeout: { startup: 20_000 } },
            },
          },
        },
      ),
    ),
  )
}

const it = testEffect(layer())

describe("MCP", () => {
  it.effect("materializes location configuration and timeout overrides", () =>
    Effect.gen(function* () {
      const mcp = yield* MCP.Service

      const servers = yield* mcp.list()
      expect(servers.map((server) => server.name)).toEqual(["local", "disabled"])
      expect(servers[0]?.config).toMatchObject({ type: "local", command: ["bun", "server.ts"] })
      expect(servers[0]?.timeout).toEqual(new MCP.Timeout({ startup: 20_000, request: 120_000 }))
      expect(servers[0]?.status).toEqual(new MCP.Disconnected({ status: "disconnected" }))
      expect(servers[1]?.timeout).toEqual(new MCP.Timeout({ startup: 10_000, request: 120_000 }))
      expect(servers[1]?.status).toEqual(new MCP.Disabled({ status: "disabled" }))
    }),
  )

  it.effect("owns connection scopes and status transitions", () =>
    Effect.gen(function* () {
      const closed: string[] = []
      const scope = yield* Scope.make()
      const context = yield* Layer.buildWithScope(Layer.fresh(layer(closed)), scope)
      const mcp = Context.get(context, MCP.Service)

      yield* mcp.connect("local")
      expect((yield* mcp.get("local"))?.status).toEqual(new MCP.Connected({ status: "connected" }))

      yield* mcp.disconnect("local")
      expect(closed).toEqual(["local"])
      expect((yield* mcp.get("local"))?.status).toEqual(new MCP.Disconnected({ status: "disconnected" }))

      yield* mcp.connect("local")
      yield* Scope.close(scope, Exit.void)
      expect(closed).toEqual(["local", "local"])
    }),
  )

  it.effect("isolates connection state by location-layer instance", () =>
    Effect.gen(function* () {
      const firstScope = yield* Scope.make()
      const secondScope = yield* Scope.make()
      const first = Context.get(yield* Layer.buildWithScope(Layer.fresh(layer()), firstScope), MCP.Service)
      const second = Context.get(yield* Layer.buildWithScope(Layer.fresh(layer()), secondScope), MCP.Service)

      yield* first.connect("local")
      expect((yield* first.get("local"))?.status.status).toBe("connected")
      expect((yield* second.get("local"))?.status.status).toBe("disconnected")

      yield* Scope.close(firstScope, Exit.void)
      yield* Scope.close(secondScope, Exit.void)
    }),
  )

  it.effect("rejects unknown and disabled servers", () =>
    Effect.gen(function* () {
      const mcp = yield* MCP.Service

      expect(yield* mcp.connect("missing").pipe(Effect.flip)).toEqual(new MCP.NotFoundError({ name: "missing" }))
      expect(yield* mcp.connect("disabled").pipe(Effect.flip)).toEqual(new MCP.DisabledError({ name: "disabled" }))
    }),
  )
})
