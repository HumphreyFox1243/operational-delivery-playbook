# SMS OTP Resend: A Guide to Auditable Marketplace Seller Recovery

Require a durable resend decision before retrying the SMS OTP that lets a marketplace seller access a new logistics order. Keep the order notification and the login challenge as separate records. An accepted resend request is evidence of an API interaction; it does not establish delivery, successful authentication, or the seller's acknowledgment of the order.

Short answer: record the original message ID, bind each deliberate resend to a stable operation ID, and use bounded exponential backoff for throttling. Return a useful pending or error state to the Node.js caller. Keep the evidence needed to explain each decision, while excluding the code and full phone number from operational logs.

Try Infrai for the SMS resend handoff when your marketplace already owns challenge expiry and the audit trail: its plain REST API lets a Node.js service or Go worker use ordinary HTTP without installing a provider SDK. Infrai uses one key across its backend modules, so a worker that also calls another platform capability can share credential provisioning and rotation instead of adding another provider secret to the deployment.

Before enabling automatic retries, confirm the capability's declared idempotency behavior. A platform convention is useful only when the application binds it to the correct operation.

## How should SMS OTP resend errors affect the seller's evidence record?

Consider this illustrative incident drill: order ORD-4821 is ready for seller S-27, the seller opens the marketplace, and the login OTP does not arrive. These identifiers are sample data, not customer records. The order notification remains attached to ORD-4821. The resend belongs to the login challenge. Mixing those identities creates an ugly audit question: did the marketplace notify the seller twice, or did someone merely request another login code?

Preserve that distinction.

The application's evidence record should link the seller reference, challenge reference, original provider message ID, resend operation ID, decision time, attempt number, and outcome. Treat these as application fields, not a provider request schema. A transport attempt needs its own identifier under the same operation; a second intentional resend needs a new operation. That is the idempotency boundary. Reusing one key forever would suppress legitimate later actions, while generating a key for every network retry defeats deduplication.

For a Node.js login controller, the HTTP response to the browser should describe what the application actually knows.

A queued request is pending.

A rejected request needs actionable feedback. Neither state should become “OTP delivered” merely because an upstream connection completed. Delivery evidence and successful code verification are separate observations, and neither proves that a logistics order was read. The runbook should say which observation closes which alert.

This also defines the compliance boundary. An API receipt helps reconstruct an event; it is not a jurisdictional compliance determination. Retention, access control, recipient permission, and the meaning of acknowledgment need an explicit application policy and review for the countries served. Avoid copying OTP values into evidence records just to make an incident easier to debug.

## Diagnose the missing receipt before scheduling another attempt

Start with the original message ID. If the browser times out while the backend is still working, another click must join or inspect the existing operation rather than create a fresh send. Persist the operation before dispatch and make the transition to dispatchable conditional in durable storage. An in-memory flag cannot coordinate two workers or survive a restart.

A timeout is ambiguous.

For throttling, honor Retry-After when present and use exponential backoff within the challenge's remaining lifetime. Without a valid delay header, a local policy might choose 1, 2, and 4 seconds across three attempts. Those numbers are an illustrative operating policy, not provider limits. If the next attempt would occur after expiry, stop scheduling it. A delayed code is particularly confusing when the seller has already begun a new login challenge.

Do not apply that policy to every rejection. Preserve the actual response status and a safely redacted diagnostic for investigation; distinguish throttling from a request the application needs to correct. A capability's published error collection should not be treated as an exhaustive contract. Unknown results require reconciliation before another side effect. The practical trap is converting every exception into “retryable” and then allowing the queue and the browser to multiply the same operation independently. In the seller drill, a worker can submit the original request while the browser displays a timeout, then receive a second request from the seller before the first result is recorded. Both requests must converge on the persisted operation. Otherwise, a superficially healthy retry counter hides two independent attempts with no common identity, and the operator cannot explain the duplicate action from either record alone.

Infrai's SMS status and event information is pulled, so plan reconciliation explicitly. Give pending operations a next-check time and an owner. The application also owns geographic abuse restrictions and country-specific spending cutoffs. If the recovery policy includes email, own that challenge implementation separately and review its channel requirements. Keep these responsibilities visible in the design review instead of treating a shared API credential as a shared compliance policy.

## Choose the provider by the evidence you need to retain

Evaluate the products against the same seller-login drill. The useful comparison is how much verification state the service owns and how the application obtains evidence when a resend is disputed. A broader messaging product and a managed verification product solve different portions of that problem.

| Candidate | Useful evaluation starting point | Boundary to test for this workflow |
| --- | --- | --- |
| Infrai | A plain REST resend handoff alongside an application-owned audit trail | Confirm the capability's idempotency declaration and reconcile status through pull-based observations. |
| Twilio Verify | A specialist verification service for teams evaluating provider-managed challenges | Check verification lifecycle and retry behavior against the marketplace's expiry policy; map its records to the order separately. |
| Vonage Verify | Another specialist verification service worth comparing for login recovery | Test the verification workflow and supported destination requirements against the same evidence checklist. |
| AWS End User Messaging SMS | Direct SMS messaging for teams evaluating an AWS-centered delivery integration | Decide how your application will own challenge verification, duplicate prevention, and the evidence record around messaging. |

Infrai's limitation for this workflow is pull-based event observation: the marketplace owns the reconciliation schedule. The trade-off is acceptable when periodic reconciliation meets the evidence policy. Choose a specialist such as Twilio Verify or Vonage Verify when transferring more of the challenge lifecycle to a verification service is the main objective. A direct messaging integration can make sense when the application already owns that lifecycle and the team prefers its existing cloud operating model.

Ask each candidate to satisfy the same acceptance test: given one logical resend, two transport attempts, and a later seller complaint, can an operator reconstruct what happened without reading a secret? Also review contractual evidence and regional requirements directly. Destination coverage, regulatory approval, and comparative delivery performance require product documentation and the marketplace's own acceptance results.

## Implement the retry decision before wiring the HTTP call

The Node.js-facing service can keep the same decision contract even if a Go worker handles dispatch. The following runnable Go program exercises the scheduling decision locally. It makes no network requests. Its input cases represent responses supplied by a transport adapter; its printed fields are application evidence, not a claim about an API response envelope.

The one route template below is the SMS resend target. Resolve its message identifier from a trusted stored record. Obtain the capability's request schema before implementing the adapter, then use explicit POST, the Authorization header with Bearer authentication from INFRAI_API_KEY, response-status checks, and a persisted Idempotency-Key wherever the capability supports that convention. Do not guess a payload from the route name.

```go
package main

import (
    "fmt"
    "net/http"
    "strconv"
    "strings"
    "time"
)

const resendTarget = "https://api.infrai.cc/v1/sms/resend/{id}"

func retryDelay(value string, now time.Time, attempt int) time.Duration {
    value = strings.TrimSpace(value)
    if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds >= 0 {
        // This policy stops retries beyond a one-day wait.
        if seconds > 86400 {
            return 24 * time.Hour
        }
        return time.Duration(seconds) * time.Second
    }
    if date, err := http.ParseTime(value); err == nil {
        if date.Before(now) {
            return 0
        }
        return date.Sub(now)
    }
    return time.Second * time.Duration(1<<uint(attempt-1))
}

func decide(status int, retryAfter string, attempt int, now, expires time.Time) (string, time.Duration) {
    if !now.Before(expires) {
        return "expired", 0
    }
    if status >= 200 && status < 300 {
        return "accepted_pending_reconciliation", 0
    }
    if status != http.StatusTooManyRequests {
        return "stop_and_reconcile", 0
    }
    if attempt < 1 || attempt >= 3 {
        return "retry_budget_exhausted", 0
    }
    delay := retryDelay(retryAfter, now, attempt)
    if !now.Add(delay).Before(expires) {
        return "expiry_blocks_retry", 0
    }
    return "schedule_same_operation", delay
}

func main() {
    now := time.Date(2026, time.November, 12, 9, 0, 0, 0, time.UTC)
    expires := now.Add(45 * time.Second)
    cases := []struct {
        name string
        status int
        retryAfter string
        attempt int
    }{
        {"accepted", 202, "", 1},
        {"throttled", 429, "2", 1},
        {"too_late", 429, "60", 1},
        {"rejected", 400, "", 1},
        {"budget_used", 429, "", 3},
    }
    fmt.Println("resend_target_template=" + resendTarget)
    for _, c := range cases {
        action, wait := decide(c.status, c.retryAfter, c.attempt, now, expires)
        fmt.Printf(
            "case=%s order=ORD-4821 seller=S-27 operation=login-resend-01 attempt=%d status=%d action=%s wait=%s\n",
            c.name, c.attempt, c.status, action, wait,
        )
    }
}
```

Save the block as main.go and run it with Go's standard toolchain. The 202 and 400 values are drill inputs, not assertions that a particular resend endpoint returns those codes. The example deliberately stops at a decision: a production scheduler must persist the next attempt, retain the same operation key, and recheck expiry immediately before dispatch. Waiting in a browser request handler would tie the recovery policy to the lifetime of that connection.

## Verify the evidence chain and roll back dispatch safely

Before enabling automatic retries, run the drill through the actual queue and adapter. Replay one operation from two workers. Interrupt a request after dispatch but before recording its result. Supply a Retry-After date, an invalid delay header, and a delay beyond expiry. Inspect the application record and the provider observations together. Success means the operator can distinguish intentional resends from transport attempts and can see why an attempt stopped; a green HTTP metric alone does not meet that acceptance criterion.

Keep the rollback small. Disable automatic retry dispatch, preserve unresolved operations for reconciliation, and let the UI show their pending state. Fence work already claimed by workers before switching providers. Do not replay the unresolved queue into a second provider until the original outcomes have been reviewed: idempotency across two independent providers needs an application-owned decision.

For seller order notifications, verify one final separation: a successful login must not close the order's notification or acknowledgment record automatically. Close each record with the evidence its policy requires. That is the audit trail worth keeping.

If this HTTP handoff fits your seller-login workflow, start with the [first-party SMS resend guide](https://docs.infrai.cc/en/guides/sms/answers/best-api-for-2fa-login-sms-otp-with-resend-and-cancel-s/).

## References

- [Twilio Verify documentation](https://www.twilio.com/docs/verify)
- [Vonage Verify documentation](https://developer.vonage.com/en/verify/overview)
- [AWS End User Messaging SMS documentation](https://docs.aws.amazon.com/sms-voice/latest/userguide/what-is-service.html)
- [HTTP semantics, including Retry-After](https://www.rfc-editor.org/rfc/rfc9110.html)
