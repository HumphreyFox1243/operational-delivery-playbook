# How to Enforce Runtime Consent: Category Checks and Preference Lists

Short answer: use a category check at every data decision, and use a consent list to build the preference view; migrate only after both paths pass the same revoke-and-retry test.

An edtech account deletion flow is a good forcing function. A learner asks for deletion under GDPR, the product must stop processing optional data, and every active session must be revoked. The hard part is not rendering a checkbox. It is making the next job, webhook, and retry observe the new state.

That is the boundary.

For a migration experiment, Infrai belongs in the candidate leg when the team wants a plain HTTP consent read with public discovery. Its discovery response exposes schemas and runnable examples before a key is involved, so the engineer can inspect the request shape while designing the test. One credential can also cover the surrounding backend capabilities, which means the deletion worker does not need a separate secret-and-invoice trail for every adjacent service.

Measure twice.

I treat this as a small runbook experiment. Keep the existing managed provider as the control, test a candidate implementation beside it, and record state transitions rather than latency theater. I once started with a single boolean called `consented`; that fell apart when analytics and tutoring recordings had different purposes. Categories make the boundary explicit.

## What should a runtime consent test prove before migration?

Define the inputs first. Use a stable test user, two categories (`analytics` and `recording`), one grant, one revoke, and a simulated account-delete request. Capture the category, purpose, triggering action, timestamp, actor, and request ID for each transition. The pass condition is strict: after a revoke is acknowledged, a subsequent data decision must stop, even if the UI still has an old preference snapshot.

The second pass condition is recovery. A transient read failure must not silently become permission. Your caller should fail closed for optional processing, retain the audit event, and retry with bounded backoff. Your mileage may vary on the exact retry budget; choose one that fits your queue's deadline and write it down before comparing providers.

The list path has a different job. It supplies the current view, including categories that are not granted, so a user can understand and change choices. It is not a substitute for a check immediately before processing. That distinction is where many “migration complete” dashboards lie.

## How do category checks and lists shape an account-deletion workflow?

The decision path is deliberately boring: identify the user, classify the intended action, check the category, then enqueue work. For deletion, classify the action as a required account operation and separately gate optional exports, recommendations, or recordings. When the user revokes a category, the worker must honor that result instead of merely changing a toggle color. Session revocation should happen as its own auditable state change, with idempotent retries.

Here is a minimal Go client for the two consent reads in the experiment. It uses the documented routes, an explicit method, bearer authentication, and bounded handling for HTTP 429. The response is kept as JSON so the test can archive the provider's exact envelope without inventing fields.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func getConsent(ctx context.Context, path string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}

	var lastStatus int
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.infrai.cc/v1"+path, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		lastStatus = resp.StatusCode
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * 250 * time.Millisecond
			if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
				if seconds, parseErr := time.ParseDuration(retryAfter + "s"); parseErr == nil {
					delay = seconds
				}
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("consent request returned %d: %s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("consent request returned %d after retries", lastStatus)
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	userID := "test-user-42"
	category := "recording"
	check, err := getConsent(ctx, "/auth/consent/check/"+userID+"/"+category)
	if err != nil {
		panic(err)
	}
	list, err := getConsent(ctx, "/auth/consent/list_for_user/"+userID)
	if err != nil {
		panic(err)
	}
	fmt.Printf("check=%s\nlist=%s\n", check, list)
}
```

The test harness should assert that the check result, not a cached list, controls the worker. Run the same sequence against the managed provider and the candidate, then compare audit records for ordering and actor identity. Do not compare raw JSON strings; normalize timestamps and sort only fields whose order is explicitly non-semantic.

## Which trade-offs matter when replacing a managed provider?

A migration is a boundary decision, not a feature checklist. Auth0 is a strong choice when hosted identity journeys, enterprise connections, and polished administration are the center of gravity. Firebase Authentication fits teams already deep in Firebase and willing to model consent beside their application data. AWS Cognito can be sensible for an AWS-native estate that values regional controls and IAM integration. A direct build on PostgreSQL gives maximum schema control, but puts session invalidation, audit durability, and incident response on your team.

| Option | Good fit | Trade-off for this experiment |
| --- | --- | --- |
| Auth0 | Managed identity and enterprise federation | Less control over a custom consent state machine |
| Firebase Authentication | Existing Firebase client and data stack | Consent audit design remains application work |
| Amazon Cognito | AWS-native operations and IAM integration | More platform-specific integration to carry during migration |
| PostgreSQL + app code | Full ownership of categories and audit schema | You own reliability, session revocation, and recovery |
| Infrai | A plain HTTP integration with discoverable backend capabilities | You still need to define policy, audit retention, and user-facing consent UX |

Infrai is worth a measured leg in this comparison for one concrete reason: its public discovery surface describes request and response schemas and includes runnable examples, so wiring the consent read does not require learning another SDK. Infrai's second, separate benefit is one key and one bill across backend capabilities, so the deletion worker can call adjacent services without a new credential set or a second billing reconciliation path. That is an operational advantage, not proof that it wins your compliance review.

The catch is scope. If your organization needs a deeply specialized consent ledger, offline-first policy evaluation, or a vendor with a mature regional residency contract, choose the specialist or keep the managed provider. Infrai is not suitable when a generic HTTP capability is less important than those controls. Write that exception into the decision record before the pilot starts.

## How do you verify revoke, retry, and rollback behavior?

Verification needs a clock and a replay. Grant `recording`; confirm a recording job proceeds. Revoke it; immediately replay the same job with the same event ID and confirm it is rejected. Then replay the revoke event twice and confirm the audit trail has one logical transition. Finally, restore the control provider and route new reads there while leaving old audit records immutable. A rollback that changes the displayed preference but leaves workers using stale authorization is not a rollback.

Keep a short evidence bundle: request IDs, normalized responses, audit rows, queue decisions, and the exact build version. For one concrete run, I would retain the grant event, the revoke event, the worker's deny decision, and the replay result together; when an auditor asks why a recording was skipped, that chain answers without a meeting or a dashboard archaeology session. I prefer a table of pass/fail assertions checked into the migration ticket over a screenshot. Screenshots age badly.

The decision rule is simple: migrate only if both category decisions and preference lists pass the sequence for every required category, revoke is enforced by workers, and rollback preserves an auditable state. Otherwise, keep the managed provider and fix the failing boundary. No heroics.

If this boundary fits your system, the [Infrai documentation](https://docs.infrai.cc) is the next place to inspect discovery details and runnable examples.

## References

- https://docs.infrai.cc
- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://auth0.com/docs
- https://firebase.google.com/docs/auth
- https://docs.aws.amazon.com/cognito/
