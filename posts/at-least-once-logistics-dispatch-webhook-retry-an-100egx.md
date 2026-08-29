# At-Least-Once Logistics Dispatch: Webhook Retry and Node.js Idempotency

Drain a rate-limited logistics worker pool with per-event delayed retries, but commit each shipment transition behind a durable idempotency key. Short answer: at-least-once delivery can present the same webhook more than once, so correctness has to come from one durable business outcome per event, not from an assumption that the queue will invoke a Node.js worker only once.

I've been paged for both sides of this failure: a job that appeared to go missing and a delivery that ran twice. The invariant that survived those reviews was blunt. A retry is another attempt at the same shipment event, never permission to create another label, dispatch notice, or carrier booking.

## Why can at-least-once webhook delivery duplicate delayed Node.js jobs?

There is an unavoidable uncertainty window between performing a business effect and recording completion. Imagine a worker draining a backlog of `shipment.ready` events while a carrier endpoint limits request rate. The worker claims an event, submits the carrier request, and then must persist its result and acknowledge the job. If execution stops anywhere in that sequence, the queue may make the event available again because it cannot infer the state of the external carrier from a missing acknowledgement. The next worker sees a valid job, not proof that the first attempt did nothing.

This isn't a scheduling error. It is the consequence of preferring another delivery attempt over silent loss.

The distinction between identity and attempt matters here. Assign a stable event key when the shipment event is accepted, and preserve it across every delayed retry. Give each attempt a separate identifier for logs and traces. If a retry receives a new business key, the consumer cannot distinguish recovery from new work; if all attempts share one trace identifier, operators cannot reconstruct the retry sequence. I use names such as `event_id=ship_7842_ready_v3` and `attempt_id=att_04`, because an incident timeline should answer both “which transition?” and “which execution?” without guessing.

The hard case is a remote side effect. A local database can usually put the processed-event marker and the shipment state mutation in one transaction. A carrier call sits outside that transaction. If the carrier accepts an idempotency key, send the stable event key on every attempt and persist the returned carrier reference. If it exposes a lookup by that reference, reconciliation can resolve an ambiguous timeout. If neither contract exists, exactly-once execution cannot be manufactured by adjusting a queue delay; the runbook must admit a residual duplicate risk and define how operators reconcile it.

Ack last.

## The incident boundary is a state machine, not a timer

The operational model needs explicit states. For this logistics flow, `accepted` means the application durably owns the event, `ready` means it may compete for worker capacity, `in_flight` means one attempt has a lease, `retry_wait` means a later attempt has been scheduled, and `completed` means the business outcome is durable. A dead-letter state can retain exhausted work for inspection, but it must not silently masquerade as completion.

Timers only decide when work becomes eligible. They don't establish ownership of a shipment transition, make a carrier operation idempotent, or prove that a prior attempt failed. This is where many retry designs go wrong: they devote careful thought to exponential backoff, then treat the handler as though delay implies uniqueness. It doesn't.

For a rate-limited pool, the latency-versus-cost decision belongs in admission control. A larger worker fleet may reduce queueing latency, but it cannot exceed the downstream rate contract and may spend most of its time waiting. A smaller steady fleet costs less but lets a burst of warehouse scans age in the queue. Set a target for oldest-ready-job age, cap concurrent calls at the destination's allowed rate, and scale workers only while both constraints permit useful work. Your mileage may vary because shipment urgency, carrier limits, and burst shape determine the right operating point; the deciding evidence is the age distribution under a representative replay, not worker CPU alone.

| Signal | What it answers | Runbook response |
| --- | --- | --- |
| Oldest ready-job age | Are shipments waiting too long? | Add useful concurrency only below the downstream limit; otherwise reduce intake or negotiate capacity. |
| Attempts per event | Is recovery becoming routine? | Inspect failures by destination and status class before raising retry limits. |
| Idempotency conflicts | Are two workers racing the same transition? | Verify the unique key and transaction boundary; keep the recorded outcome. |
| Completion rate by event age | Is the pool actually draining? | Compare arrivals with durable completions, not with handler starts. |
| Dead-letter count | Has automatic recovery ended? | Reconcile business state before replaying anything. |

Watch the tail. An average queue delay can look healthy while one warehouse or carrier partition remains stuck, so alerting should preserve destination and age dimensions without putting unbounded event identifiers into metric labels.

## Put the duplicate barrier before the business effect

The preventative path should be boring enough to review during an incident. The following Go example models the database boundary behind a generic worker. `BeginEvent` represents an atomic insert protected by a unique constraint on the stable event ID. A duplicate returns the recorded result when it is complete; an event already owned by another worker is released for a later attempt. The carrier client receives the same idempotency key every time.

```go
package worker

import (
	"context"
	"errors"
	"time"
)

var ErrBusy = errors.New("event is already in flight")

type Job struct {
	EventID   string
	ShipmentID string
	AttemptID string
}

type Record struct {
	Completed  bool
	CarrierRef string
}

type EventStore interface {
	// BeginEvent atomically creates or locks the stable event record.
	BeginEvent(ctx context.Context, eventID string) (Record, error)
	Complete(ctx context.Context, eventID, carrierRef string) error
	Release(ctx context.Context, eventID string, nextAttempt time.Time) error
}

type Carrier interface {
	Book(ctx context.Context, shipmentID, idempotencyKey string) (string, error)
}

type Handler struct {
	store   EventStore
	carrier Carrier
	now     func() time.Time
}

func (h Handler) Deliver(ctx context.Context, job Job) (string, error) {
	record, err := h.store.BeginEvent(ctx, job.EventID)
	if err != nil {
		return "", err
	}
	if record.Completed {
		return record.CarrierRef, nil
	}

	carrierRef, err := h.carrier.Book(ctx, job.ShipmentID, job.EventID)
	if err != nil {
		next := h.now().Add(backoff(job.AttemptID))
		if releaseErr := h.store.Release(ctx, job.EventID, next); releaseErr != nil {
			return "", releaseErr
		}
		return "", err
	}

	if err := h.store.Complete(ctx, job.EventID, carrierRef); err != nil {
		return "", err
	}
	return carrierRef, nil
}

func backoff(attemptID string) time.Duration {
	// Production policy can add bounded jitter and classify retryable failures.
	return 30 * time.Second
}
```

This interface intentionally leaves queue acknowledgement outside `Deliver`. The caller acknowledges only after `Complete` succeeds. It also leaves the transaction implementation unspecified: the important property is atomic exclusion on `EventID`, not a particular database. In a real implementation, a lease needs an expiry and an owner token so a stale worker cannot complete a record after another worker legitimately takes it over. That fencing check belongs in `Complete` and `Release`.

Don't retry every failure. Authentication and malformed payload failures need correction or dead-letter handling, while transient capacity responses belong on the delayed path. Backoff should be bounded and jittered so a recovered destination doesn't receive the entire logistics backlog at once. The classification policy is part of the service contract, and changing it deserves the same review as changing concurrency.

The callback destination is a security boundary too. When webhook URLs can be influenced by a tenant, follow the OWASP SSRF guidance: prefer an allowlist where the business permits one, validate the scheme and destination, account for DNS resolution and redirects, and restrict outbound network access. A delayed worker is still a server making a server-side request. Moving the request out of the API process doesn't remove that trust boundary.

## Test the failure windows before tuning throughput

Start with a replay test that delivers the same event concurrently to two workers. The unique event record should admit one business transition, and both calls should converge on the same stored result. Then terminate a worker after it acquires the event but before the carrier call, after the carrier call but before local completion, and after completion but before acknowledgement. Those three cuts expose different recovery paths. The middle one is the reason the downstream idempotency contract matters.

Next, load the pool with the burst shape a warehouse actually produces and hold the fake carrier to its configured rate. Measure oldest-job age, completions, retry volume, and downstream calls. A test that reports only handler throughput can pass while shipment latency grows without bound. Keep the assertions business-shaped: each accepted shipment transition eventually reaches either one durable completion or an explicit terminal review state, and no retry creates a second carrier booking.

Deployment needs its own gate. Roll out the schema and unique constraint before code that depends on them, keep old and new workers compatible with the stable event key, and canary with duplicate injection enabled. During rollback, don't delete processed-event records merely because an earlier binary doesn't read every new column. They are operational evidence and part of the correctness boundary.

The advice is not universal. A queue is a poor choice when the business requires a single global schedule rather than per-event eligibility, or when a multi-step process needs durable compensation and human approval semantics that a simple job handler would have to recreate. Stick with a periodic scheduler for a bounded reconciliation sweep. Use a workflow model when the unit of recovery is an ordered business process rather than one idempotent webhook effect. For low-value notifications where duplicates are acceptable and latency dominates, a lighter deduplication window may be enough, but write that tolerance into the service objective instead of implying exactly-once behavior.

## The release decision

Ship the retry path only when a stable event key crosses every boundary, the database rejects concurrent ownership, acknowledgement follows durable completion, the remote destination has an idempotency or reconciliation contract, and fault tests cover the ambiguous completion window. Then tune the worker pool against oldest-job age and the downstream rate ceiling.

If any one of those properties is absent, more workers merely reach the unsafe boundary faster. Hold the release, or narrow the promised outcome.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Server_Side_Request_Forgery_Prevention_Cheat_Sheet.html
- https://docs.bullmq.io/
