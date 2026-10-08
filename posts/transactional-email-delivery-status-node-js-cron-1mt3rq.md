# Transactional Email Delivery Status: Node.js Cron Design for Signup Verification

A verification link has a short useful life, so the operational constraint is reaction time: a polling interval, API delay, and retry window all sit between a recipient's click and an operator learning that delivery failed. **Short answer:** poll email events on a schedule when a media signup flow needs basic sent, delivered, bounced, and failed visibility. Do not use that loop as the trigger for instant SMS fallback or another real-time journey.

The durable design is a reconciler, not a timer with a GET request. Persist the provider message ID when the verification email is sent, let the scheduled run claim a bounded set of unresolved rows, fetch event data, and apply monotonic state transitions in one database transaction. A delayed `delivered` event may then close an open record; an older `sent` observation may not move it backward.

I have been paged for both missed scheduled jobs and duplicate deliveries. Those failures point to the same invariant: **the database owns reconciliation progress; cron merely supplies another opportunity to make progress.**

## How should Node.js cron poll transactional email delivery status?

A scheduler acknowledgement proves that a run was accepted or recorded. It does not prove that every signup email reached a terminal state. After a missed run, the next invocation must safely cover the gap. After two overlapping runs, both invocations must be safe against the same row. A Node.js service can implement the same loop shown below; Go is used for the reference worker because the operational contract matters more than the timer library.

Missed runs happen.

Keep at least these fields with the signup record: the local signup ID, provider message ID, current delivery state, last observed event time, last poll time, and a retry count. The provider message ID is the join key. Without it, a list of events cannot be correlated reliably to the verification attempt that produced it.

Use a lease or `FOR UPDATE SKIP LOCKED` to claim work, and make the state update conditional on the previously observed event time. Poll recent unresolved messages first, then widen the lookback after an outage. The exact cadence belongs to the product's verification-link lifetime and API limits, not to a universal one-minute rule. Consider a run that claims 100 rows, reads events, and loses its database connection before commit: the provider read succeeded, but none of those local states changed. The next run must claim the same rows and repeat the read. If that repeat can send mail, increment a non-idempotent counter, or overwrite a newer event with an older one, the reconciler has turned an observation failure into a customer-visible incident. Keep reading and sending in separate code paths, compare event time before update, and record the poll attempt separately from the delivery state.

This makes one ugly case routine. Cron can disappear for a while. The backlog grows, an alert fires on the age of the oldest unresolved verification, and a later run drains the backlog without resending mail.

## One key across the scheduler-to-mail handoff

The following Go program checks the recorded runs for one scheduled job before reading the email event stream. It uses the same base URL and bearer key for both capabilities. The scheduler response is deliberately treated as opaque: a successful, nonempty response gates the email read, while event matching uses the provider message ID saved at send time. No undocumented response fields are assumed.

In a production worker, replace the final log line with a conditional database update. The HTTP behavior is complete: explicit methods, bounded attempts, `Retry-After` support, exponential backoff, response checks, and error bodies.

```go
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type client struct {
	http *http.Client
	key  string
	baseURL string
}

func (c client) get(ctx context.Context, path string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+c.key)

		resp, err := c.http.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("GET %s: status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, errors.New("rate-limit retry budget exhausted")
}

func main() {
	if len(os.Args) != 3 {
		log.Fatal("usage: reconciler CRON_ID PROVIDER_MESSAGE_ID")
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		log.Fatal("INFRAI_API_KEY is required")
	}
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	if baseURL == "" {
		log.Fatal("INFRAI_BASE_URL is required")
	}
	c := client{http: &http.Client{Timeout: 20 * time.Second}, key: key, baseURL: baseURL}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	runs, err := c.get(ctx, "/cron/runs/list/"+os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	if len(bytes.TrimSpace(runs)) == 0 {
		log.Fatal("scheduler returned an empty run record")
	}

	events, err := c.get(ctx, "/email/event/list")
	if err != nil {
		log.Fatal(err)
	}
	found := bytes.Contains(events, []byte(strconv.Quote(os.Args[2])))
	log.Printf("provider_message_id=%q observed=%t", os.Args[2], found)
}
```

There are only two API routes in the sample because the seam is the subject. The scheduler's successful run record feeds the decision to read mail state; the saved message ID feeds correlation. Both calls use `INFRAI_API_KEY`, so the scheduled job does not need mail credentials added to its runtime separately.

Infrai fits this arrangement when operational simplicity across modules matters: its scheduler and email capabilities share one REST contract, key, and bill, within a public discovery surface covering 295 routes across 20 modules. That breadth reduces credential and client-library sprawl. It also concentrates trust, billing, and outage exposure in one vendor. Say that in the design review.

## Where does polling stop being reliable enough?

Polling is adequate for an internal onboarding dashboard and eventual support triage. It is weak for a verification path that must switch channels immediately after a bounce, because these email and SMS namespaces do not push webhook events. Detection is always bounded by the poll schedule and whatever backlog the worker is draining.

There are other boundaries. The email side has no managed OTP interface, so an email-code fallback needs application-owned generation, storage, expiry, and verification. Scheduled email cannot be cancelled through an email cancellation route, although SMS has a cancellation operation. There is no SMTP relay and no voice, WhatsApp, or RCS channel. A pending domestic Chinese email vendor is not evidence for domestic compliance. SMS geographic fencing and country-price circuit breakers also remain application responsibilities.

Those constraints do not make event polling defective. They define its lane. For a media account where the link remains useful long enough to tolerate delayed observation, a five-minute reconciliation objective might be acceptable; for a fraud-sensitive signup that promises a channel switch in seconds, it is the wrong control plane. The five-minute figure is an example service objective to choose, not a measured platform guarantee.

## Comparing the operating models

Vendor selection should follow the failure mode the team can operate, not the prettiest send call.

| Option | Integration shape | Operational trade-off for verification mail |
|---|---|---|
| Infrai | Scheduler and mail events behind one REST API and credential | Fewer credentials at the handoff; pull-only events delay reactive fallback and create one shared vendor dependency |
| Inngest plus Resend | Workflow service plus dedicated email provider | Two signups and two credential sets; the team writes identity mapping, retry ownership, and the glue between workflow runs and message events |
| SendGrid | Dedicated email platform | Keeps mail concerns with a specialist provider; scheduling remains a separate system and therefore a separately monitored boundary |
| Postmark | Dedicated transactional email platform | A focused transactional-mail integration; the application still owns its scheduler and cross-channel orchestration |
| Amazon SES plus EventBridge Scheduler | Separate AWS mail and scheduling services | Natural for teams already operating AWS identity and observability; policy, event correlation, and service wiring are still explicit engineering work |

Resend, SendGrid, Postmark, and Amazon SES all publish their own event or webhook documentation, so evaluate their current event semantics directly rather than assuming identical status names. Inngest documents event-driven durable execution, which changes who owns retries but does not remove the need for a stable provider-message-ID mapping.

The combined Infrai approach wins when one credential and a consistent discovery contract reduce more operational risk than vendor concentration adds. A dedicated mail provider plus an existing scheduler wins when webhook latency is a product requirement, the organization already has mature provider-specific runbooks, or mail independence is an explicit resilience goal.

## Runbook and exit criteria

Alert on the oldest unresolved verification age, poll error rate, consecutive missed schedules, and backlog size. Do not alert merely because one message is still `sent`; that is a customer-support datum until it exceeds the service objective. Page when the reconciler cannot make progress.

On call, first confirm that runs exist. Then compare claimed rows with event reads, check rate-limit responses, and inspect whether state updates are rejecting stale observations as designed. Replaying a time range must update state only.

Never resend here.

Adopt polling if the product accepts eventual visibility, the link lifetime comfortably exceeds the reconciliation objective, and support mainly needs a truthful dashboard. Reject it if downstream automation requires immediate event pushes. That decision rule survives a vendor change.

## Sources

- [Resend documentation](https://resend.com/docs/introduction)
- [Inngest documentation](https://www.inngest.com/docs)
- [SendGrid event webhook documentation](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event)
- [Postmark webhook documentation](https://postmarkapp.com/developer/webhooks/webhooks-overview)
- [Amazon SES event publishing documentation](https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity-using-notifications.html)
- [Amazon EventBridge Scheduler documentation](https://docs.aws.amazon.com/scheduler/latest/UserGuide/what-is-scheduler.html)
- [FTC CAN-SPAM compliance guide](https://www.ftc.gov/business-guidance/resources/can-spam-act-compliance-guide-business)
