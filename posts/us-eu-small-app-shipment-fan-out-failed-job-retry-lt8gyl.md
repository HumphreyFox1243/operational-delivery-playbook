# US/EU Small-App Shipment Fan-Out: Failed Job Retry, Queue, DLQ Redrive, or Database Polling?

**Short answer:** for a small marketplace app sending one shipment update to many subscribers, start with a database-backed retry record if the volume is low and delayed recovery is acceptable; move delivery to a queue with a DLQ and controlled redrive when latency, fan-out, or operator load becomes the dominant risk.

The decision is not really “cheap versus expensive.” It is how much failure state the application team wants to own. I have been paged by missed jobs and duplicate deliveries, and the pattern is consistent: the first version works until a worker disappears between the external send and the acknowledgement. Then the retry mechanism becomes the incident.

This note compares the two designs for a small app deployed across US and EU regions. The shipment event is the concrete case: `shipment-7842` changes to `in_transit`, and the service must fan that update out to email, webhook, and partner subscribers without silently dropping one of them.

## What should a small app choose for failed job retry: a queue or manual database polling?

Manual database polling has a strong early advantage: the job, attempt count, next-attempt time, and failure reason can live beside the business record. A single query can show an operator what is due. For a small app with a few hundred pending deliveries, this may be the lowest total cost because there is no separate delivery service to provision or monitor.

The catch is that the table is not a retry engine by itself. The application must implement claiming, leases, backoff, concurrency limits, stale-worker recovery, and poison-job isolation. Two pollers must not claim the same shipment subscriber at once. A crashed worker must not hold a row forever. A failed webhook must not keep a whole batch from reaching the other subscribers.

A queue makes those delivery states more explicit. The worker receives one subscriber task, applies the effect idempotently, and acknowledges only after the durable success boundary. Repeated failures can be isolated in a dead-letter queue (DLQ), where an operator can inspect the reason and redrive corrected work. That reduces the amount of scheduling machinery in the application, but it does not remove the need for an idempotency key or an audit record.

| Design | Latency and cost profile | Failure state the team owns |
| --- | --- | --- |
| Manual database polling | Low setup cost; latency depends on poll interval and database headroom | Claims, leases, backoff, fairness, stuck rows, and poison rows |
| Queue with DLQ redrive | Better fit for prompt fan-out and independent subscribers; adds a delivery component | Idempotent consumers, DLQ review, retention policy, and redrive controls |
| Scheduled trigger plus either design | Fine for periodic enqueueing or cleanup; poor as the delivery record itself | Missed-trigger policy and a separate worker path |

The US/EU label does not change the semantics. It changes the operating questions: where the subscriber endpoint is, which region owns the database row, and how much cross-region delay the latency budget can absorb. Keep the decision about failure handling, not geography.

## The incident boundary: where does a retry become a duplicate shipment update?

The dangerous sequence is short. A worker consumes `shipment-7842/email-17`, calls the email provider, and loses its process before recording success. The queue or poller retries. The second worker cannot tell whether the first call completed, so the customer receives two updates. In the incident review, the missing fact is usually not “did the worker return an error?” It is “did the provider accept the request before the worker vanished?” Those are different questions, and a green worker metric can hide the second one. I learned to put the delivery ID in the provider request, the database row, and the log line before tuning concurrency; otherwise the team spends the recovery window arguing over whether two messages mean two attempts or two business effects.

It doesn't need to be mysterious.

The inverse ordering is also bad: mark the task complete before calling the provider, and a process stop can lose the update. Acknowledging first only hides the problem. The durable record and the external effect need a deliberate idempotency contract; a database transaction alone cannot make an unrelated email or webhook call atomic. That boundary is where “failed job retry” stops being a scheduling detail and becomes a business correctness rule. If a subscriber endpoint returns a transient error, retry it. If the response is unknown after a timeout, reconcile it by delivery ID before sending again. If the payload is permanently invalid, quarantine it with the original shipment and subscriber identifiers instead of letting it consume the normal retry budget.

In a postmortem, I want one identifier on every line: shipment ID, subscriber ID, attempt number, and delivery ID. “The queue is healthy” is not enough. The useful question is whether the business effect is safe to repeat and whether the operator can prove what happened.

Here is the consumer-side shape. The in-memory ledger is only a readable model; production code needs an atomic datastore operation, and the queue acknowledgement belongs after `ApplyOnce` returns success.

```go
package main

import (
	"fmt"
	"sync"
)

type Delivery struct {
	ID       string
	Shipment string
	Target   string
}

type Ledger struct {
	mu      sync.Mutex
	applied map[string]struct{}
}

func NewLedger() *Ledger {
	return &Ledger{applied: make(map[string]struct{})}
}

func (l *Ledger) ApplyOnce(d Delivery, send func(Delivery) error) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.applied[d.ID]; ok {
		return false, nil
	}
	if err := send(d); err != nil {
		return false, err
	}
	l.applied[d.ID] = struct{}{}
	return true, nil
}

func main() {
	ledger := NewLedger()
	delivery := Delivery{ID: "shipment-7842/email-17", Shipment: "shipment-7842", Target: "email-17"}

	for attempt := 1; attempt <= 2; attempt++ {
		applied, err := ledger.ApplyOnce(delivery, func(d Delivery) error {
			fmt.Printf("send %s to %s\n", d.Shipment, d.Target)
			return nil
		})
		if err != nil {
			fmt.Printf("attempt %d: retain for retry: %v\n", attempt, err)
			continue
		}
		fmt.Printf("attempt %d: acknowledge; applied=%t\n", attempt, applied)
	}
}
```

There is a subtle limitation here. Holding a mutex while calling an external provider is unsuitable for a real worker pool. The example keeps the ordering visible, not the production locking strategy. Use a datastore record with an atomic claim and an idempotency key accepted by the downstream system when that system provides one. If it does not, choose an effect that can be reconciled, or accept that “at least once” delivery cannot guarantee exactly-once external behavior.

## How do queue, DLQ redrive, and polling compare for shipment fan-out?

For fan-out, create one delivery task per subscriber rather than one task containing an opaque list. One bad partner endpoint then cannot block email and webhook delivery. A queue gives each task its own retry and DLQ fate. A database poller can do the same, but the schema and worker code must preserve that granularity. I would rather inspect twelve explicit rows than one “shipment fan-out” row whose payload contains twelve hidden outcomes; the latter makes a partial failure look like a single failure and makes a redrive decision much harder to defend during an on-call handoff.

The latency-cost trade-off is clearest in the poll interval. A ten-second poll gives a rough lower bound on ordinary pickup delay and adds repeated database reads even when no work is due. A sixty-second poll costs fewer reads but makes the shipment status feel stale. Queue delivery usually pays more operational complexity up front to avoid making the database the clock, claim mechanism, and work buffer at once.

DLQ redrive needs guardrails. Redrive only after identifying the failure class, and send corrected work through the same idempotent consumer path as a first delivery. Limit the redrive batch, watch downstream rate limits, and retain the original delivery ID in the audit record. Redrive is an operator action, not proof that the payload is safe.

Scheduled triggers are useful for enqueueing due work or running reconciliation. They are a poor substitute for a durable delivery record. A scheduled workflow can start a process, but the process still needs its own retry state, ownership, and idempotency boundary. The scheduling documentation for GitHub Actions is a useful reminder that a trigger is an execution event, not an application queue.

## When is manual polling the better operational choice?

Use manual polling when pending work is small, the database is already the system of record, and a few seconds or minutes of extra latency is acceptable. It is also a good fit when operators need SQL-level inspection more often than they need high delivery throughput. Keep the poller boring: claim a bounded batch, use a lease, calculate backoff, write an attempt record, and release or complete the claim.

Do not choose it when subscriber count can grow sharply, when a single regional database is already near its write limit, or when delayed recovery would violate a marketplace commitment. A poller is especially risky if the team cannot state how a crashed worker is reclaimed. Stick with a queue when independent subscriber work, burst absorption, and controlled poison-message handling matter more than minimizing moving parts.

Your mileage may vary. The right threshold is measured from the service's latency budget, database capacity, and on-call tolerance, not from a generic “small app” label.

## A runbook for choosing and operating the design

Before implementation, write down the delivery contract in plain language:

- What is the stable idempotency key for one shipment and one subscriber?
- How long may a shipment update wait before it is an incident?
- Which errors are retryable, and which payloads go straight to operator review?
- How will US and EU workers avoid claiming the same delivery?
- What record proves the final disposition after a redrive?

Then test the boundaries deliberately: kill a worker after the provider call, pause the poller, make one subscriber return a permanent error, and redrive a single corrected task. Verify that the other subscribers continue, that duplicate delivery is harmless, and that the dashboard distinguishes “not attempted” from “attempted but unknown.”

That is the decision rule I would put in the runbook: database polling for low-volume, inspectable work with tolerant latency; a queue and DLQ redrive for independent fan-out where latency and recovery control justify the extra component. Neither design makes a shipment update exactly once by magic. The application still has to make repetition safe.

## References

- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fifo-queues.html
- https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows
