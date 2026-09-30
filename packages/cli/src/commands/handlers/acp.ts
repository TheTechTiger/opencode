import { ndJsonStream } from "@agentclientprotocol/sdk"
import { OpenCode } from "@opencode/client/promise"
import { Service } from "@opencode/client/effect/service"
import { Effect } from "effect"
import { ACP } from "../../acp/agent"
import { Commands } from "../commands"
import { Runtime } from "../../framework/runtime"
import { Standalone } from "../../services/standalone"

export default Runtime.handler(
  Commands.commands.acp,
  Effect.fn("cli.acp")(function* () {
    process.env.OPENCODE_CLIENT = "acp"
    const endpoint = yield* Standalone.start()
    const client = OpenCode.make({ baseUrl: endpoint.url, headers: Service.headers(endpoint) })
    const output = new WritableStream<Uint8Array>({
      write: (chunk) =>
        new Promise<void>((resolve, reject) => {
          process.stdout.write(chunk, (error) => (error ? reject(error) : resolve()))
        }),
    })
    const connection = yield* ACP.connect(client, ndJsonStream(output, yield* stdin))
    const exited = yield* Effect.raceFirst(
      Effect.promise(() => connection.closed).pipe(Effect.as(undefined)),
      endpoint.exited,
    )
    // EOF owns this stdio process; exiting also closes the private server's lease pipe.
    if (!exited) return yield* Effect.sync(() => process.exit(0))
    const reason = "code" in exited ? `code ${exited.code}` : `signal ${exited.signal}`
    // stdout carries ACP, so the diagnostic goes to stderr.
    yield* Effect.sync(() => {
      process.stderr.write(`opencode acp: server exited unexpectedly (${reason})\n`)
      process.exit(1)
    })
  }),
)

const stdin = Effect.suspend(() => {
  const source: { controller?: ReadableStreamDefaultController<Uint8Array> } = {}
  const data = (chunk: Buffer) => source.controller?.enqueue(new Uint8Array(chunk))
  const end = () => source.controller?.close()
  const error = (cause: Error) => source.controller?.error(cause)
  return Effect.acquireRelease(
    Effect.sync(() => {
      const stream = new ReadableStream<Uint8Array>({ start: (controller) => void (source.controller = controller) })
      process.stdin.on("data", data).on("end", end).on("error", error)
      return stream
    }),
    () =>
      Effect.sync(() => {
        process.stdin.off("data", data).off("end", end).off("error", error)
        process.stdin.pause()
      }),
  )
})
