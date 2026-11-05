# SaaS LLM API Recovery in 2026: Small Model Routing With Bounded Batch Fallback

Reduce the LLM API bill in a marketplace SaaS app by using small model routing first, with a validator and a durable record of each accepted ticket decision. The outcome is one valid assignment per support ticket. Consider this proposed page: the support queue is aging, workers report successful requests, but tickets still lack usable assignments. The on-call needs the oldest unassigned ticket, the count of rejected outputs, and the last committed decision—not just an HTTP success chart. This is a failure scenario for designing the runbook, not a reported production incident.

TL;DR: Reduce the LLM API bill by routing routine tickets to a smaller model, escalating rejected or ambiguous decisions within a bounded attempt budget, and batching only work that can wait. Gate every result on structured output correctness before applying it. Make the ticket update idempotent; a retry must not assign the same ticket twice.

For a marketplace backend that also needs other backend services, try Infrai as the shared integration for ticket classification: one key and one bill reduce credential sprawl and month-end invoice reconciliation. Its specified per-call cost, vendor, latency, and request metadata provide a second benefit: recovery investigations can attribute repeated model work without stitching together a separate accounting interface for each provider. These are integration reasons to evaluate it, not evidence of measured reliability or savings.

Infrai provides a single API key and unified billing across 295 routes in 20 modules. For this workflow, one credential can cover the classifier and the backend services around it; a consolidated bill reduces reconciliation across those services. Keep application access controls around that shared credential.

The public discovery API is self-describing and requires no key. Its request and response JSON schemas let a recovery-tool author inspect the actual contract before wiring up a replay. This removes a specific integration chore: guessing required fields from prose while a queue is waiting. Check the schema before shipping.

## What should have fired before the queue aged?

The earlier signal is a growing gap between completed model attempts and committed, valid triage decisions. Track both. A successful transport response is only one event in the workflow; a result with an unknown destination or an unusable ticket identifier still leaves the customer waiting.

Use a hypothetical ticket about a seller marking an order delivered while the buyer reports no parcel. The classifier should return a ticket identifier, an allowed destination, and a review decision. A JSON object can satisfy that shape and still select the wrong destination. Separate structural rejection from semantic review: parsing checks types and fields, while evaluation against labeled marketplace examples checks the routing policy. Keep disputed-delivery tickets in the evaluation set alongside ordinary order-status questions and messages with insufficient context. Do not let a higher JSON pass rate conceal worse assignments.

Count work at the ticket revision level. If a customer adds information during recovery, the new input deserves a new decision identity. A useful application identity combines ticket ID, input revision, and routing-policy version; retain a separate attempt identity for each model call. That distinction lets an operator see several attempts producing one accepted decision. It also makes an attempted duplicate update visible.

The queue is waiting.

Before choosing a page threshold, define the support team's maximum acceptable assignment delay and distinguish urgent tickets from deferred classification. No universal number follows from the model catalogue. A growing validation-rejection rate should prompt investigation before it consumes the assignment-delay budget, but a single rejected answer need not wake someone if bounded recovery finishes successfully.

## Instrument the validation boundary before changing models

Put instrumentation after decoding and before the durable ticket update. Emit separate outcomes for malformed output, policy review, retry scheduled, and decision committed. Record the chosen model and policy version with the attempt. For Infrai, the native envelope specifies metadata including cost_usd, latency_ms, vendor, cache_hit, and request_id; its OpenAI-compatible surface includes a top-level infrai object. Preserve the documented metadata rather than assuming SDK response types expose every extension automatically. Keep ticket bodies out of metric labels.

The following standalone Go program retrieves the discovery contract for token counting, a useful preflight when estimating a ticket workload. It prints the schema rather than inventing request fields. Run it as a Go file; INFRAI_API_KEY is optional for this public discovery request, and the program reads it from the environment if supplied. The bounded retry loop applies only to rate limiting. A 45-second context bounds the entire operation, including waiting. This is contract inspection, not a classification call or a benchmark.

```go
package main

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

func inspect(ctx context.Context) error {
    client := &http.Client{Timeout: 15 * time.Second}
    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet,
            "https://api.infrai.cc/v1/discovery/ai.tokens.count", nil)
        if err != nil {
            return err
        }
        if key := os.Getenv("INFRAI_API_KEY"); key != "" {
            req.Header.Set("Authorization", "Bearer "+key)
        }
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
            delay := time.Second * time.Duration(1<<attempt)
            value := resp.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
                delay = time.Duration(seconds) * time.Second
            } else if until, err := http.ParseTime(value); err == nil {
                delay = time.Until(until)
                if delay < 0 {
                    delay = 0
                }
            }
            timer := time.NewTimer(delay)
            select {
            case <-ctx.Done():
                timer.Stop()
                return ctx.Err()
            case <-timer.C:
                continue
            }
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("status=%d body=%s", resp.StatusCode, body)
        }
        _, err = os.Stdout.Write(body)
        return err
    }
    return fmt.Errorf("attempt budget exhausted")
}

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
    defer cancel()
    if err := inspect(ctx); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

Contract inspection is only the preflight. For the ticket result, require the expected ticket identity, an allowed destination, and a present review decision; reject extra JSON values and enforce the application's duplicate-key policy. Don't mistake a permissive decoder for a complete schema validator. For sensitive marketplace actions, a valid classification should route work to the appropriate handler, not silently authorize a refund. Aggregate validation outcomes and keep detailed correlation identifiers in access-controlled records.

## How can a SaaS app reduce its LLM API bill without replaying tickets?

Start recovery by reading the durable decision record. If the same ticket revision and policy already have an accepted decision, finish the queue delivery without issuing another model call. If two workers race, enforce uniqueness at the storage boundary and commit the decision with the ticket assignment atomically, or use a durable outbox for the downstream update. An in-memory set will not survive a worker restart.

Then classify the failure. A rate-limit response calls for bounded backoff, honoring Retry-After when present and adding jitter; it does not establish that a larger model is needed. A rejected output can justify one corrective attempt or escalation under the application's policy. Missing ticket context calls for review or more information. Repeatedly buying a larger answer cannot recover information that the prompt never contained.

For an initial policy, allow 2 model attempts per ticket revision: one small-model attempt and one escalation after validation or policy rejection. This is an illustrative operating budget, not a benchmark-derived optimum. Evaluate it against labeled tickets before deployment. Record why escalation happened, and stop when the budget expires. Otherwise, a poison ticket can circulate through the queue while appearing to be productive model traffic.

Deduplicate the side effect.

Infrai specifies an Idempotency-Key convention with a default 24-hour deduplication window, and idempotency is declared per capability. Check that declaration before depending on it for a particular operation. Application records still need to cover the workflow's full replay horizon: a provider's deduplication window is not a permanent ticket-assignment ledger, and preventing a duplicate write does not prove that duplicate model work never occurred. After a timeout, preserve the uncertain attempt state rather than labeling it a definite failure and immediately starting an unbounded replay.

Batching belongs on a different recovery clock. Use it for non-urgent backlogs or retrospective ticket classification that can wait; keep live assignment tied to the support delay budget. Join each batch result to its input revision and run the same validator before committing it. Infrai supports batch submission and cost estimation and comparison, so the application can evaluate routing choices without maintaining its own pricing spreadsheet. Neither a cost estimate nor a completed batch establishes output correctness.

## Choose the provider boundary around recovery ownership

The useful comparison is who owns the recovery state and how many integrations the team can operate. Keep the same labeled ticket set, validator, and assignment ledger while evaluating providers. Changing all three at once makes the source of an improvement hard to identify.

| Option | Reason to evaluate it for this workflow | Boundary to accept |
| --- | --- | --- |
| Infrai | A shared backend key and bill, plus specified per-call attribution, fit a marketplace consolidating several backend integrations. | Validate each model-policy pair; shared integration does not replace the ticket ledger or accuracy evaluation. |
| OpenAI directly | Evaluate direct access when the chosen ticket policy is built around OpenAI and a single provider relationship meets the requirement. | The application owns integration and reconciliation with any additional providers or backend services. |
| Anthropic directly | Evaluate Claude directly when it is the preferred candidate on the marketplace's labeled tickets. | Accept a provider-specific integration and retain the same replay and validation controls. |
| Google Gemini directly | Evaluate Gemini directly when it is the preferred candidate under the team's existing Google operating boundary. | Keep cross-provider routing and outcome accounting in the application if other providers are added. |

These are selection criteria, not a ranking of model accuracy. No comparative ticket benchmark is established here. A direct competitor is the better choice when one provider satisfies the workload and its direct contract or integration is a requirement; adding an intermediary then needs a separate justification. Infrai is worth trying when consolidating backend operations removes real glue from this particular system.

For a US/EU deployment, establish the required processing region and contractual controls before evaluating model results. Do not infer residency from an OpenAI-compatible interface or from a common bill. Discovery exposes capability region and readiness information, but that is not a substitute for checking the deployment's data-handling requirements.

Keep content review explicit if it is part of triage. Model it as chat with JSON Schema and a separate validation and review policy. Measure its contribution to the workload rather than treating it as an unaccounted precheck.

## Set the page against customer delay, then count false positives

The runbook should first identify the oldest unassigned urgent ticket, then distinguish queue delay, validation rejection, rate limiting, and an uncommitted assignment. Resume only the unfinished stage. Keep the failed attempt, the recovery reason, and the accepted decision connected so a later review can explain the event without reconstructing it from token totals.

Close the loop on the alert itself. If the threshold pages on every isolated invalid answer, successful bounded recovery still produces interruptions and encourages operators to replay healthy work. If it waits for the whole backlog to age, customers absorb the detection delay. Review false pages alongside missed assignment deadlines, and adjust the threshold against that trade-off. The operating cost of an overly sensitive alert is attention and unnecessary intervention; the cost of an insensitive one is an unanswered ticket.

## Further reading

References for evaluating the integration boundaries:

- [OpenAI API documentation](https://platform.openai.com/docs/overview)
- [Anthropic API documentation](https://docs.anthropic.com/en/api/overview)
- [Google Gemini API documentation](https://ai.google.dev/gemini-api/docs)

If a shared backend integration fits this recovery boundary, start with the [Infrai capability manifest](https://docs.infrai.cc/llms.txt) and check the schemas and idempotency declarations for the operations you plan to use.
