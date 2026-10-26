# durable-workflow-lab

An order saga on [Temporal](https://docs.temporal.io/), tested against a real Temporal dev server, with the recorded event histories published as a timeline viewer.

**Live demo:** https://gerardrecinto.github.io/durable-workflow-lab/ shows the actual event history of six runs: happy path, transient failure retried, declined card, late failure unwinding earlier steps, cancel signal during a durable timer, and a worker that dies mid-activity while a second worker resumes.

Personal learning project. I have not run Temporal in production. Nothing here is mocked: tests start the Temporal CLI dev server (server 1.32) and use the same history and replay machinery as a production cluster.

## What it demonstrates

- **Durable execution.** The workflow function keeps no state. Progress is the event history on the server, so a new worker rebuilds it by replay. `TestWorkerLossResumesFromHistory` stops the first worker while a charge call is in flight; the second worker finishes the order, the reservation is not repeated, and the card is charged once.
- **Retries belong to the platform.** `TestTransientFailureIsRetriedWithoutWorkflowCode`: the payment provider fails twice, Temporal retries with backoff, and the workflow code has no retry loop. A card decline is marked non-retryable and is attempted exactly once.
- **Sagas.** Compensations are kept on a stack and run in reverse (`refund` then `release`) when shipping fails or the customer cancels, on a disconnected context so they still run if the workflow itself is cancelled.
- **Signals and durable timers.** A cancel signal races a timer inside a `Selector`; the signal wins and cancels the timer.
- **At-least-once activities need idempotency.** After the lost worker the charge runs again. An idempotency key on the charge is what keeps it to one.
- **Determinism and versioning.** `TestRecordedHistoriesReplayOnCurrentCode` replays every recorded history against the current code. `TestVersionedChangeStillReplaysOldHistories` adds a fraud-check step behind `workflow.GetVersion` and old histories still replay. `TestUnversionedChangeBreaksReplay` adds the same step with no version marker and the replayer rejects it (error code `TMPRL1100`).

## Run it

```
go test ./... -race -count=1     # starts a real dev server; first run downloads the Temporal CLI
go run ./cmd/scenarios           # re-records testdata/*.json and docs/histories/*.json
python3 -m http.server -d docs   # then open http://localhost:8000
```

## Things I observed

- A 300 ms durable timer fired about a second after it started in the dev server (history event 17 to 18 on the happy path). Sub-second timers are coarse there, so the tests and timeline use windows with slack and nothing asserts on timer precision.
- The non-determinism error text varies with where the mismatch is found, so the test matches the `TMPRL1100` code rather than a phrase.
- My first version of the crash scenario let the abandoned charge attempt finish and report back, so Temporal never retried it and the scenario proved nothing. I changed it so the first attempt never reports, which forces the timeout and retry.
- To simulate a crash in one process the first worker is stopped with a short stop timeout while its activity is blocked. That is an approximation of a killed process, not the same as one.

## Compared with Cadence and Windmill

From their documentation, not from running them: Cadence is the system Temporal was forked from and shares the workflow-as-code and event-history model. Windmill is a script and flow orchestrator where flows are defined as steps with retries and approvals, rather than deterministic code replayed from history. I have used neither.

## References

- Temporal: [Workflows](https://docs.temporal.io/workflows), [Workflow definition and determinism](https://docs.temporal.io/workflow-definition), [Activities](https://docs.temporal.io/activities), [Retry policies](https://docs.temporal.io/encyclopedia/retry-policies), [Event history](https://docs.temporal.io/encyclopedia/event-history)
- Temporal Go SDK: [Versioning](https://docs.temporal.io/develop/go/versioning), [Testing suite](https://docs.temporal.io/develop/go/testing-suite), [package documentation](https://pkg.go.dev/go.temporal.io/sdk)
- Hector Garcia-Molina and Kenneth Salem, [Sagas](https://www.cs.cornell.edu/andru/cs711/2002fa/reading/sagas.pdf) (SIGMOD 1987); [the Saga pattern](https://microservices.io/patterns/data/saga.html)
- [Cadence](https://cadenceworkflow.io/) and [Windmill](https://www.windmill.dev/docs/intro) documentation
