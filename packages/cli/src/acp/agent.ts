import { agent, RequestError, type Stream } from "@agentclientprotocol/sdk"
import { ClientError, type OpenCodeClient } from "@opencode/client/promise"
import { Cause, Effect } from "effect"
import { ACPConnection } from "./connection"
import { ACPError } from "./error"
import { ACPService } from "./service"

// Untraced so request spans parent to the caller's span instead of a setup span that has already ended.
export const connect = Effect.fnUntraced(function* (client: OpenCodeClient, stream: Stream) {
  const run = Effect.runPromiseWith(yield* Effect.context<never>())
  const handle = <Params, A>(name: string, call: (params: Params, signal: AbortSignal) => Promise<A>) => {
    const handler = Effect.fn(name)(function* (params: Params, signal: AbortSignal) {
      return yield* Effect.tryPromise({
        try: () => call(params, signal),
        catch: (cause) =>
          cause instanceof ClientError && cause.reason === "Transport" ? new ACPError.ServerUnavailableError() : cause,
      })
    }, Effect.catchCause(toRequestError))
    return (ctx: { readonly params: Params; readonly signal: AbortSignal }) => run(handler(ctx.params, ctx.signal))
  }
  const connection = agent({ name: "opencode" })
    .onRequest(
      "initialize",
      handle("ACP.initialize", (params) => service.initialize(params)),
    )
    .onRequest(
      "authenticate",
      handle("ACP.authenticate", (params) => service.authenticate(params)),
    )
    .onRequest(
      "session/new",
      handle("ACP.session.new", (params) => service.newSession(params)),
    )
    .onRequest(
      "session/load",
      handle("ACP.session.load", (params) => service.loadSession(params)),
    )
    .onRequest(
      "session/list",
      handle("ACP.session.list", (params) => service.listSessions(params)),
    )
    .onRequest(
      "session/delete",
      handle("ACP.session.delete", (params) => service.deleteSession(params)),
    )
    .onRequest(
      "session/resume",
      handle("ACP.session.resume", (params) => service.resumeSession(params)),
    )
    .onRequest(
      "session/close",
      handle("ACP.session.close", (params) => service.closeSession(params)),
    )
    .onRequest(
      "session/fork",
      handle("ACP.session.fork", (params) => service.forkSession(params)),
    )
    .onRequest(
      "session/set_config_option",
      handle("ACP.session.set_config_option", (params) => service.setSessionConfigOption(params)),
    )
    .onRequest(
      "session/set_mode",
      handle("ACP.session.set_mode", (params) => service.setSessionMode(params)),
    )
    // The SDK signal is passed through rather than interrupting the fiber: a cancelled turn still resolves with
    // `stopReason: "cancelled"`.
    .onRequest(
      "session/prompt",
      handle("ACP.session.prompt", (params, signal) => service.prompt(params, signal)),
    )
    .onNotification(
      "session/cancel",
      handle("ACP.session.cancel", (params) => service.cancel(params)),
    )
    .connect(stream)
  // Inbound dispatch starts after the stream's async read loop yields, so handlers never observe this before assignment.
  const service = ACPService.make({ client, connection: ACPConnection.make(connection) })
  return connection
})

const toRequestError = Effect.fnUntraced(function* (cause: Cause.Cause<unknown>) {
  const error = Cause.squash(cause)
  if (error instanceof RequestError) return yield* Effect.fail(error)
  if (ACPError.is(error)) return yield* Effect.fail(ACPError.toRequestError(error))
  yield* Effect.logError("ACP request failed", cause)
  return yield* Effect.fail(ACPError.toRequestError(ACPError.fromUnknown(error)))
})

export * as ACP from "./agent"
