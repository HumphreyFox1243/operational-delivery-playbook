# Healthtech Renewal Deadline: Node.js Webhook Deduplication with a FIFO Idempotency Key

Short answer: treat the queue as a delivery mechanism, not as the deduplication authority. For a healthtech renewal reminder, assign one durable event identity before enqueueing, make the consumer claim that identity transactionally, and retain the record long enough to cover late retries. A five-minute suppression window can reduce duplicate work; it cannot prove that a reminder was sent once.

That is the operational constraint. The reminder is tied to a business deadline, and a missed reminder is bad. A duplicate reminder can be worse when it confuses a customer or causes a support escalation. The recovery design has to handle both without pretending that ordering alone provides exactly-once effects.

## What should a Node.js renewal workflow do when a webhook is duplicated?

Start with the business identity, not the message identity. For example, `renewal:acme:2026-09-30` can identify one reminder decision if the business rule says that this account and deadline produce one reminder. The exact format is less important than its stability and ownership. A retry of the same decision must reuse the same key; generating a new UUID in the retry path creates a second event in the system's eyes.

Persist a small idempotency record before the external side effect. A useful state model is `processing`, `sent`, and `retryable` or `failed`, with timestamps, the event key, the recipient reference, and the response metadata needed for an audit. Put a unique constraint on the logical key. Two workers may race, but only one should win the claim. The losing worker should acknowledge or reschedule according to the recorded state, rather than send a second reminder. You don't need a complicated queue topology for this, but you do need a clear owner for the record and a rule for what happens when the record is stale.

Keep the transaction boundary honest. Claiming a row and sending an HTTP request are not one atomic operation, so a process can die after the receiver accepts the request but before the queue acknowledgement. Recovery therefore needs a lease or a reviewable retry rule. If the receiver supports idempotency keys, pass the same key downstream. If it does not, make the local record the business-effect boundary and involve an operator when the state is ambiguous.

Five minutes is a test case, not a retention policy.

For delayed work, test a duplicate immediately, one at four minutes, and one after five minutes. The latter should still be suppressed by the durable consumer record. Then stop a worker after it claims the record and stop another after the outbound request returns. Those crash points reveal more than a happy-path assertion that the queue accepted a message. In a Node.js service, the queue adapter, database transaction, and HTTP client must all preserve the same key; the language does not change that invariant. A useful rehearsal follows one renewal from deadline calculation through enqueue, delivery, receiver response, and reconciliation: record the event key at each hop, compare the first and second delivery timestamps, inspect the claim transaction, and confirm that the receiver's request ID maps back to one business decision. If the first request was accepted but the process lost its response, the replay must be classified as ambiguous rather than silently counted as a fresh send.

## A small Go consumer that makes the recovery boundary visible

The following example keeps the queue and storage interfaces generic. It shows the important ordering: claim the event, perform the side effect with the same idempotency key, and record the outcome. A production implementation should make `Claim` atomic in the database and give a stuck `processing` row an expiry that an operator can inspect.

```go
package main

import (
	"context"
	"errors"
	"fmt"
)

var ErrAlreadyHandled = errors.New("event already handled")

type Event struct {
	Key       string
	Recipient string
	Payload   []byte
}

type Store interface {
	Claim(ctx context.Context, key string) error
	MarkSent(ctx context.Context, key string, responseID string) error
	MarkRetryable(ctx context.Context, key string, reason string) error
}

type Sender interface {
	Send(ctx context.Context, recipient string, payload []byte, idempotencyKey string) (string, error)
}

func Handle(ctx context.Context, store Store, sender Sender, event Event) error {
	if event.Key == "" || event.Recipient == "" {
		return fmt.Errorf("invalid renewal event")
	}

	if err := store.Claim(ctx, event.Key); err != nil {
		if errors.Is(err, ErrAlreadyHandled) {
			return nil
		}
		return fmt.Errorf("claim %q: %w", event.Key, err)
	}

	responseID, err := sender.Send(ctx, event.Recipient, event.Payload, event.Key)
	if err != nil {
		if markErr := store.MarkRetryable(ctx, event.Key, err.Error()); markErr != nil {
			return fmt.Errorf("send %q: %v; record retry state: %w", event.Key, err, markErr)
		}
		return fmt.Errorf("send %q: %w", event.Key, err)
	}

	if err := store.MarkSent(ctx, event.Key, responseID); err != nil {
		return fmt.Errorf("record sent state for %q: %w", event.Key, err)
	}
	return nil
}
```

The code does not claim magical exactly-once delivery. It makes an uncertain handoff observable. If `Send` succeeded and `MarkSent` did not, a retry can still be dangerous unless the receiver deduplicates the key or an operator reconciles the request ID. That is the part worth documenting in the runbook, because a green queue metric cannot tell you whether a customer saw one reminder or two.

Protect the key as an identifier, not as a secret. The key should not contain a patient's name, diagnosis, or other sensitive data. If it is derived from protected business data, use a controlled derivation and follow the application's key-management practices. OWASP recommends managing cryptographic keys through their full lifecycle; the same discipline is useful here for credentials and signing material, even though an idempotency key itself is usually a correlation value rather than a password.

## How do FIFO queues, webhook retries, and a five-minute dedupe window interact?

Ordering helps workers observe related messages in a predictable sequence. It does not stop a producer from publishing twice, and it does not undo an external HTTP effect after a worker crashes. A dedupe window can collapse repeated submissions close together, but it has a finite memory by definition. The consumer record supplies the longer memory needed for delayed tasks.

The queue contract should be written down in plain terms:

| Concern | Required decision | Recovery evidence |
|---|---|---|
| Ordering | Choose a stable grouping key only where sequence matters | Logs show the group and sequence at receipt |
| Duplicate publish | Reuse the business event key on every retry | One durable claim exists for the key |
| Duplicate delivery | Make the consumer claim atomic | Competing workers produce one winner |
| Receiver response | Classify success, retryable failure, and ambiguity | Response code and request ID are retained |
| Late replay | Retain the idempotency record beyond the queue window | A post-window replay has no new side effect |

Do not use a FIFO setting to hide an unclear business rule. If two reminders for the same account are genuinely different, they need distinct event keys. If they are retries of one reminder, they need one key. The queue cannot infer that distinction from payload similarity without a documented policy.

Rate limiting belongs in the same design review. HTTP 429 means the server is asking the client to slow down, and a `Retry-After` response header may indicate when to try again. Backoff should preserve the event key, cap the number of attempts, and emit a useful alert when the deadline is approaching. A retry loop that creates a fresh key on each attempt is a duplicate generator with better logging.

## Verify, deploy, and roll back without losing the audit trail

Before production, exercise the recovery matrix in a staging environment with representative, non-sensitive data. Assert the business effect at the receiver, the idempotency row in storage, the queue acknowledgement, and the alert stream together. Checking only queue depth is how duplicate delivery survives a test suite.

The minimum cases are deliberately unglamorous: a missing key, malformed payload, concurrent delivery, a receiver rate limit, a permanent receiver rejection, a late replay after the five-minute window, and a worker termination after the receiver responds. Add a case where the renewal deadline passes while the message is retrying. The correct action may be to suppress the reminder, escalate it, or send it with a revised template; that is a product policy, not a queue feature.

Deploy the producer and consumer changes with the same event-key schema. During rollback, stop new publishing or route it through the last tested path, but keep the idempotency records. Deleting those records erases the evidence that prevents replay from becoming a second business effect. Reconcile ambiguous sends from stored request IDs before draining or reprocessing messages.

The approach is not suitable when the reminder is one step in a long-running workflow with joins, human approvals, or compensation across several systems. In that case, stick with a workflow engine that owns those state transitions, or use a scheduler with durable execution semantics; a FIFO queue plus a table will leave too much orchestration in application code. It is also a poor fit when strict ordering has no business meaning, because the extra ordering constraint can reduce throughput without improving correctness. Choose a standard at-least-once queue there, while keeping the same idempotent consumer.

Watch four signals: age of the oldest pending reminder, retry count by reason, records stuck in `processing`, and the ratio of sent reminders to unique event keys. Page on deadline risk and stuck claims, not on a transient queue-size spike alone. Your mileage may vary on thresholds because tenant volume and deadline policy differ; measure a week of normal traffic before setting alert bounds.

The decision rule is compact: choose ordered delivery only where sequence changes the business result, choose durable idempotency for every external effect, and choose a different workflow engine when the job needs long-running steps, joins, or human approval rather than one delayed reminder. A simple queue is suitable when recovery can be expressed as claim, send, record, and reconcile. It is not suitable when the state machine is larger than the runbook.

## References

- [MDN: HTTP 429 Too Many Requests](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429)
- [OWASP Key Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Key_Management_Cheat_Sheet.html)
