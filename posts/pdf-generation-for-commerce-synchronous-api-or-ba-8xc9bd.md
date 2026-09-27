# PDF Generation for Commerce: Synchronous API or Background Job for Batch Throughput

Send e-commerce bundle merges and splits through a background-job API by default, and reserve synchronous generation for requests with a small, enforced work budget. Batch throughput is the deciding constraint: a checkout receipt may finish within an interactive deadline, while a seller's 4,000-order export can occupy renderers, storage connections, and memory long enough to delay every request behind it.

**TL;DR:** expose job submission, status, cancellation, and artifact retrieval as separate operations. Admit work against bounded queue and renderer capacity, assign an idempotency key before enqueueing, and publish the PDF only after validation. Keep a synchronous route only when the service can reject inputs above a measured page, byte, or document-count ceiling before rendering begins.

## Should PDF generation use a synchronous API or background job?

The dangerous input is not merely a large PDF. It is an unpredictable unit of work. A bundle can contain many small packing slips, a few image-heavy return labels, or malformed source documents that take very different amounts of CPU and memory. HTTP request duration hides those differences until proxies, clients, or workers reach their own deadlines.

Then retries arrive.

If the client cannot tell whether a timed-out merge committed, retrying can create duplicate artifacts and duplicate downstream notifications. Raising the timeout only keeps scarce workers occupied longer. It does not add admission control, establish ownership after the connection closes, or tell an operator which order bundle is stuck.

The split case has another sharp edge: one input may fan out into hundreds of outputs. Returning all results in one response couples rendering, object publication, and network transfer to the same deadline. A background job gives that fan-out a durable identity and lets the system report partial internal progress without exposing partial output as complete.

## Put a work budget in front of the queue

Classify before accepting. The admission layer should inspect trustworthy metadata such as source byte count, document count, requested split count, and known page count. It should reject impossible requests, route proven-small work to the synchronous pool, and enqueue everything else. Do not use an estimate as a promise: encrypted, damaged, or image-dense inputs can still behave badly, so both pools need hard concurrency and memory limits.

The useful comparison is operational, not stylistic:

| Property | Synchronous path | Background-job path |
|---|---|---|
| Best fit | One bounded receipt or label | Seller exports and variable bundle fan-out |
| Completion signal | Successful HTTP response | Terminal job state plus artifact metadata |
| Retry boundary | Same idempotency key for the operation | Same idempotency key for submission |
| Capacity control | Dedicated worker pool and strict input ceiling | Queue admission, concurrency cap, and backpressure |
| Failure handling | Fail before publishing an artifact | Retry a stage; publish only the validated result |

The background-job approach has real limitations: it costs more API surface, persistent state, queue operations, artifact cleanup, and client polling or notification logic. That trade-off is justified when long-tail jobs would otherwise consume interactive capacity. For a low-volume internal tool with uniformly tiny inputs, a synchronous endpoint can remain the simpler choice, provided its limits are measured and enforced rather than documented as a suggestion.

## Make submission idempotent and publication atomic

Treat the client-supplied operation key as a uniqueness constraint scoped to a tenant. The stored request fingerprint prevents the same key from being reused for different inputs. Submission returns the existing job when both key and fingerprint match; a mismatch is a conflict. That rule closes the ambiguous-timeout gap without pretending delivery is exactly once.

The worker can retry. Side effects cannot.

This focused Go sketch shows the state transition that matters. `CreateOrGet` must be a transaction backed by a unique constraint, and `Publish` must make the final object visible before the job is marked succeeded.

```go
package jobs

import (
	"context"
	"crypto/sha256"
	"errors"
)

var ErrKeyConflict = errors.New("idempotency key reused with different input")

type Request struct {
	TenantID      string
	IdempotencyKey string
	SourceIDs     []string
	Operation     string // "merge" or "split"
}

type Job struct {
	ID          string
	Fingerprint [32]byte
	State       string
}

type Store interface {
	CreateOrGet(ctx context.Context, tenantID, key string, fingerprint [32]byte) (Job, bool, error)
	Enqueue(ctx context.Context, jobID string) error
}

func Submit(ctx context.Context, store Store, req Request, canonicalInput []byte) (Job, error) {
	fingerprint := sha256.Sum256(canonicalInput)
	job, created, err := store.CreateOrGet(ctx, req.TenantID, req.IdempotencyKey, fingerprint)
	if err != nil {
		return Job{}, err
	}
	if job.Fingerprint != fingerprint {
		return Job{}, ErrKeyConflict
	}
	if created {
		if err := store.Enqueue(ctx, job.ID); err != nil {
			return Job{}, err
		}
	}
	return job, nil
}
```

Canonical input must include every field that changes the output: ordered source identities and immutable versions, merge or split instructions, page ranges, locale, template revision, and rendering options. Hashing only filenames is a quiet data-corruption bug because a name can stay constant while its bytes change.

Store attempts separately from the logical job. A retry increments the attempt but retains one job identity. Render into a private temporary object, validate it, then promote it to an immutable final key. The Portable Document Format is standardized by ISO 32000-2; conformance to the expected PDF structure is a better publication gate than trusting a file extension or a nonzero byte count.

## Operate for throughput, not queue cosmetics

Queue depth alone is a weak signal. Watch admitted documents per minute, pages completed per minute, oldest-job age, time in each stage, active renderer count, retry rate, cancellation latency, and output validation failures. Slice them by operation and workload class. Ten image-heavy merges can matter more than a thousand one-page splits.

Set alerts on user impact and stalled progress. A growing oldest-job age while workers are saturated means capacity or admission policy is losing the race; a growing age with idle workers points toward dispatch or dependency failure. Keep interactive and batch pools separate so a seller export cannot block checkout documents. Weighted scheduling can protect small jobs, but cap their share as well or large bundles may starve indefinitely.

Backpressure must be visible at submission time. When the system has exhausted its bounded backlog, reject or defer new work with a retryable response and a stable operation key. Accepting unlimited work creates a misleading success response followed by an unbounded recovery window.

Cost belongs in capacity planning, though price should not decide the API shape. Measure CPU-seconds, peak memory, temporary bytes, final bytes, and stage duration per workload class. Those figures reveal whether the bottleneck is rendering, transfer, or retention and give the team a defensible concurrency limit.

## Verify the rollout and keep rollback boring

Before shifting production traffic, replay a sanitized corpus that covers single receipts, mixed-size order bundles, high fan-out splits, corrupted inputs, encrypted documents, duplicate submissions, cancellation races, and worker termination between upload and state commit. Compare page count, expected ordering, checksums where deterministic output is guaranteed, and structural validation. Load tests should include bursts, because an average arrival rate will not expose queue admission failures.

Roll out by workload class. Start with large seller exports on the asynchronous route while leaving bounded interactive documents unchanged. Track oldest-job age and completion throughput, then move the threshold only after the queue drains predictably under a burst. The rollback is routing new submissions back to the prior path; already accepted jobs must keep their identifiers and finish or reach an explicit terminal state.

Never relabel an accepted job as absent during rollback. Clients may be polling it, and operators need the audit trail.

The decision rule is compact: use synchronous generation only when input work is bounded, the caller needs the bytes immediately, and the entire operation fits inside a deliberately protected interactive budget. Use background jobs for variable merges, high fan-out splits, bursty batches, or any workflow that needs durable retries and independent artifact retrieval. Throughput stays predictable because the queue becomes a controlled boundary rather than a place to hide latency.

## References

- ISO, "ISO 32000-2:2020, Document management — Portable document format — Part 2: PDF 2.0": https://www.iso.org/standard/75839.html
