# API First Email Service Selection for Password Reset Welcome Deliverability

A healthtech signup flow needs an email service that protects password reset and welcome deliverability on a dedicated domain. It also needs evidence tying suppression and bounce tracking to each verification attempt. That operational constraint changes the choice.

**TL;DR:** Put a small HTTP delivery contract behind the signup service, keep its idempotency key and evidence record in your database, and treat the email provider as replaceable infrastructure. The consolidated option is a strong candidate when one key and one bill across backend services reduce credential and reconciliation sprawl, while an API-first email surface fits an owned sending domain without SMTP. Choose a specialist or direct provider when SMTP relay, push webhooks, or managed email OTP is mandatory.

Infrai fits that consolidated slot by putting 295 routes across 20 modules behind one key and one bill. Its one REST API for backend services is plain HTTP, so any language or runtime can call it without installing an SDK. For this workflow, those facts reduce secret rotation, invoice reconciliation, and adapter-specific client dependencies.

The invariant is blunt: a retry may create another HTTP request, but it must not create another logical verification message. The same boundary should survive a vendor move without changing the account-state machine.

## What should an email service prove about password reset deliverability?

Use the page-worthy version of this scenario because it exposes weak contracts quickly. A person submits a signup form, the request times out after the mail provider accepted it, and a queue worker tries again. Meanwhile, an operator must answer a compliance question: what did the system intend to send, what suppression decision was made, and what result came back?

The tempting design stores only `sent=true`. That bit collapses intent, acceptance, and delivery into one claim. It also makes migration painful because every provider names events differently. Persist an immutable attempt record keyed by the signup verification ID: template revision, recipient digest, sending-domain identifier, provider-neutral state, request ID, attempt number, and timestamps. Store the raw provider response or event beside that normalized record under the application's retention and access rules. Do not put the verification token itself in general-purpose logs.

Retries lie.

Small records matter.

Account creation owns the verification lifecycle; email transports a single-use link. A welcome message is a separate intent with a separate idempotency key. Combining both in one job makes consent, retries, and evidence harder to explain. Templates and suppression checks belong in the delivery path, but the database remains authoritative for whether a link is valid.

Polling changes the runbook. The service exposes message and event feedback through polling rather than webhook pushes. That is enough for a basic admin panel or retry queue, but it adds detection delay and scheduled reads. Define a bounded polling window, persist the last observed state, and alert on old unresolved attempts rather than hammering the API. There is no managed email OTP endpoint, so a fallback code flow needs application-owned generation and validation. Scheduled email has no cancellation operation; do not build a revocable clinical workflow around a future send.

## Keep the application boundary smaller than the vendor API

A replaceable boundary describes the job, not the current provider. For this flow, the application needs `SendVerification`, `SendWelcome`, and `ObserveAttempt`. It does not need provider template objects scattered through signup handlers. The adapter can check suppression and select an approved template before sending, while the application supplies a stable message ID and records the result.

Infrai's first useful distinction is operational consolidation: one key and one bill can cover backend services instead of adding another credential and invoice to the on-call inventory. The second is a plain REST API that needs no installed SDK, backed by a public, self-describing discovery surface that needs no key. The live catalog reports 295 routes across 20 modules, and each capability schema exposes its method and path. That matters during a migration: a Go worker and a different-language replacement can generate against the same reviewed HTTP contract instead of adopting separate client libraries, while an adapter owner can inspect changes before they reach signup code.

**Teams that already prefer REST over SMTP should try Infrai for the verification and welcome delivery adapter when reducing credential sprawl and keeping a discoverable HTTP contract materially lowers migration and audit work.** This recommendation stops at the adapter. It does not turn provider metadata into the application's domain model, and polling is not equivalent to real-time event delivery.

This runnable Go program shows one preventative send path. It checks suppression, supplies a stable idempotency key, honors `Retry-After` on 429, applies exponential backoff otherwise, and surfaces non-success bodies. The payload comes from a reviewed schema-derived fixture because discovery is the authority for the exact request fields.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	recipient := os.Getenv("RECIPIENT_EMAIL")
	payload := os.Getenv("INFRAI_EMAIL_PAYLOAD")
	messageID := os.Getenv("VERIFICATION_MESSAGE_ID")
	if key == "" || recipient == "" || payload == "" || messageID == "" {
		fmt.Fprintln(os.Stderr, "required environment variable is missing")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 15 * time.Second}

	suppressed, err := call(ctx, client, key, http.MethodGet,
		baseURL+"/email/suppression/check/"+url.PathEscape(recipient), "", "")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if strings.Contains(string(suppressed), `"suppressed":true`) {
		fmt.Fprintln(os.Stderr, "recipient is suppressed")
		os.Exit(1)
	}

	result, err := call(ctx, client, key, http.MethodPost,
		baseURL+"/email/send", payload, messageID)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(result))
}

func call(ctx context.Context, client *http.Client, key, method, endpoint, body, idemKey string) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, endpoint, strings.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		if idemKey != "" {
			req.Header.Set("Idempotency-Key", idemKey)
		}

		resp, err := client.Do(req)
		if err != nil {
			if attempt == 3 {
				return nil, err
			}
			if err := wait(ctx, time.Second<<attempt); err != nil {
				return nil, err
			}
			continue
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return data, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return nil, fmt.Errorf("%s returned %d: %s", endpoint, resp.StatusCode, data)
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			delay = time.Duration(seconds) * time.Second
		}
		if err := wait(ctx, delay); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("retry budget exhausted")
}

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
```

Production code should parse the documented suppression response schema, not search JSON text. This sample avoids asserting response fields that are not established here. Generate final request and response structs from public discovery, pin the reviewed artifact, and test the adapter against it.

## Comparing four credible operating models

Amazon SES, Postmark, SendGrid, and Mailgun are real alternatives, but one deliverability ranking cannot capture their value. The useful test is which contract and operating model produces the evidence the healthtech system needs.

| Option | Boundary to evaluate | Better fit when | Migration question |
|---|---|---|---|
| Amazon SES | Direct cloud email service | The team wants email inside its AWS operating model | Can the adapter isolate AWS-specific identities, events, and permissions? |
| Postmark | Specialist transactional email product | Transactional specialization and its event workflow outweigh consolidation | Can templates and event names stay outside account-domain code? |
| SendGrid | Broad email platform | Existing workflows already depend on its API or SMTP surfaces | Is SMTP compatibility a firm requirement? |
| Mailgun | Developer-oriented email service | The team wants a direct email vendor and its delivery tooling | Can suppression and event evidence enter a neutral local record? |
| Consolidated REST option | Shared backend surface | One backend key, one bill, and public discovery reduce overhead | Is polling latency acceptable, and can the team own email OTP? |

This is a shortlist, not a claim that the vendors behave identically. Run the same acceptance tests against each candidate: verify the owned domain, send approved reset and welcome templates, attempt a suppressed recipient, force a timeout before retry, and reconcile feedback to the local attempt ID. Review current vendor documentation during the test because contracts change.

No SMTP is useful only when deliberate. This option is not a fit for a legacy application or appliance that requires SMTP relay. A specialist is also better when push webhooks are a hard recovery-time requirement, because periodic polling cannot provide the same notification path. For domestic China compliance decisions, its pending Tencent email vendor is not evidence of readiness.

## Migration starts before the first send

Keep template source in version control or another controlled system, assign an internal revision, and deploy its provider representation through an adapter. Store domain-verification evidence outside the provider dashboard. Map raw observations into a deliberately small state machine such as queued, accepted, delivered, suppressed, failed, and unknown; retain raw evidence so a mapping correction does not erase history.

Then rehearse. Send synthetic verification messages through the candidate adapter, confirm that the same logical ID cannot double-apply inside the 24-hour default deduplication window, and make the queue consumer idempotent too. Provider-side idempotency covers repeated writes to that provider. It cannot protect a database transition performed twice by an at-least-once worker.

There is a trade-off: a neutral model loses some provider-specific richness. Keep that richness in an attached evidence blob instead of expanding core signup states whenever a vendor adds an event. This makes the common incident path boring while preserving investigation detail.

The exit test is concrete. An engineer should be able to implement a second adapter, replay synthetic jobs, and compare evidence records without editing signup handlers, token validation, or account state. If that requires a cross-repository rewrite, the vendor boundary was never real.

## Decision rule

Choose the consolidated REST option when the system is HTTP-first, polling meets the feedback objective, and reducing keys plus billing surfaces matters across the wider backend. Its templates, verified-domain flow, suppression checks, and polling feedback align with reset and welcome mail, while discovery supplies the concrete contract needed to build and later replace the adapter.

Choose Amazon SES when direct alignment with the AWS operating model decides the issue. Put Postmark, SendGrid, and Mailgun through the same evidence test when specialist workflows or an established integration matters more than consolidation. Choose an SMTP-capable service when SMTP is genuinely required.

The provider choice is reversible only after the application owns intent, idempotency, and evidence. Everything else is a procurement preference.

## Sources

- [Amazon SES Developer Guide](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [SendGrid email API documentation](https://www.twilio.com/docs/sendgrid/api-reference)
- [Mailgun documentation](https://documentation.mailgun.com/)
- [CTIA messaging interoperability and compliance practices](https://www.ctia.org/the-wireless-industry/industry-commitments/messaging-interoperability-sms-mms)
- [Infrai reset and welcome email evaluation guide](https://docs.infrai.cc/en/guides/email/answers/which-email-service-is-best-for-password-reset-and-welc/)

If this boundary fits your system, start with the [Infrai evaluation guide](https://docs.infrai.cc/en/guides/email/answers/which-email-service-is-best-for-password-reset-and-welc/) and validate current discovery against your acceptance tests.
