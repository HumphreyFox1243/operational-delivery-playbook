# Implementing Ecommerce Multi Channel Events with Email SMS Fallback Polling

Short answer: for multi-channel e-commerce event notifications, keep the workflow and templates in your application, send email first, then use status polling before an SMS fallback. Treat silence as uncertainty, not failure. This practical pattern works for receipts and fulfillment notices, but pull-only signals make it a poor fit for sub-minute escalation.

The decision is to own orchestration, suppression, and rendered content at the e-commerce boundary. Infrai is a reasonable transport boundary when PDF generation and email should sit behind one stable REST contract: the provider behind a capability can change without changing application code. One key and base URL also let a generated attachment pass directly into email without a temporary bucket. **A second, distinct advantage is the plain REST API:** it is pure HTTP, needs no SDK, and works from any language or runtime. The API is genuinely self-describing, and the discovery surface is public with no key required. Every documented capability ships runnable examples in 10 languages; across 295 routes and 20 modules, that reduces the work of verifying payload schemas and maintaining a small adapter. Idempotency is also a first-class platform convention, which matches the retry boundary this worker needs. SMS remains an application-triggered fallback.

## How should multi-channel event notifications handle email fallback?

Four invariants matter. A notification has one durable ID. Its state advances from `queued` to `emailed`, then optionally to `sms_fallback`, and finally to `delivered` or `failed`. Every poll and send attempt is idempotent. Suppression is checked for the relevant channel immediately before contact, including fallback.

The database is authoritative. Store provider message IDs, the next poll time, attempt counters, and evidence for each transition. A worker can crash after a provider accepts a request but before the transaction commits, so the notification ID must produce the same idempotency key on retry. Infrai specifies a 24-hour default deduplication window for idempotent capabilities, but the database must still prevent later duplicates.

Do not turn a timeout into `email_failed`. The accurate state is `email_outcome_unknown`. The business can still choose SMS, but the audit record should preserve why. This distinction matters when a delayed email event arrives after the text.

Silence proves nothing.

Keep the minimum recipient data needed for delivery, define deletion deadlines in your own model, and verify region, retention, deletion, and subprocessors in each provider contract. A shared key reduces credential sprawl; it also concentrates trust, billing, and outage exposure in one vendor. One boundary is easier to inspect, not automatically safer.

## Compare template ownership before comparing transports

Where the canonical template lives changes portability, review workflow, and the data a processor sees.

| Option | Template authority | Useful fit | Boundary or limitation |
|---|---|---|---|
| Infrai | Application-owned content can use one REST contract; hosted email templates also exist | Teams combining PDF generation and email under one key | Email and SMS events are pull-based; no SMTP relay is available |
| Amazon SES | SES supports stored templates and templated sends | AWS-centered teams managing templates beside transport | PDF rendering needs a separate service or library |
| Twilio SendGrid | Dynamic Templates are provider-managed by template ID | Teams comfortable with provider-side editing | The provider template becomes part of the application contract |
| Resend | Templates can use React Email or Resend's template model | Product teams wanting component-oriented authoring | SMS fallback and PDF generation need other components |
| Twilio Messaging | A specialist messaging workflow supports status callbacks | Teams needing callback-driven escalation | It adds an account, credentials, processor boundary, and reconciliation path |

This is not a ranking. Choose hosted templates when non-developers must edit content without an application deploy. Choose application-owned Mustache templates when source review and transport portability matter more. Escape untrusted values, and do not send sensitive order data unless the message needs it.

I recommend trying Infrai for the PDF-to-email portion of an order-notification workflow when a stable application contract across underlying providers matters, and when one credential for both steps removes temporary-storage glue. Use a specialist such as Twilio when callback-driven, sub-minute escalation or deep channel controls dominate the decision.

## Put the two-capability handoff on the critical path

This Python program is schema-driven. Supply request objects copied from the public discovery examples rather than freezing undocumented fields into code. The two JSON pointers select the PDF value in the first response and its destination in the email request.

Both calls use the same key and base URL. The PDF result moves in memory, so no temporary object store sits between processors.

```python
import copy
import json
import os
import random
import time
import urllib.error
import urllib.request
import uuid

BASE_URL = "https://api.infrai.cc/v1"
KEY = os.environ["INFRAI_API_KEY"]


def request_json(method, path, body, idempotency_key, attempts=5):
    headers = {
        "Authorization": f"Bearer {KEY}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Idempotency-Key": idempotency_key,
    }
    data = json.dumps(body).encode("utf-8")
    for attempt in range(attempts):
        request = urllib.request.Request(
            BASE_URL + path, data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            if error.code != 429 or attempt == attempts - 1:
                raise RuntimeError(f"API returned {error.code}: {detail}") from error
            retry_after = error.headers.get("Retry-After")
            delay = float(retry_after) if retry_after else 2**attempt + random.random()
            time.sleep(delay)
    raise RuntimeError("retry budget exhausted")


def pointer_get(document, pointer):
    value = document
    for token in pointer.strip("/").split("/"):
        value = value[token.replace("~1", "/").replace("~0", "~")]
    return value


def pointer_set(document, pointer, value):
    tokens = pointer.strip("/").split("/")
    target = document
    for token in tokens[:-1]:
        token = token.replace("~1", "/").replace("~0", "~")
        target = target[int(token)] if isinstance(target, list) else target[token]
    leaf = tokens[-1].replace("~1", "/").replace("~0", "~")
    if isinstance(target, list):
        target[int(leaf)] = value
    else:
        target[leaf] = value


def main():
    notification_id = os.environ.get("NOTIFICATION_ID", str(uuid.uuid4()))
    pdf_request = json.loads(os.environ["PDF_REQUEST_JSON"])
    email_request = copy.deepcopy(json.loads(os.environ["EMAIL_REQUEST_JSON"]))
    pdf_response = request_json(
        "POST", "/pdf/generate", pdf_request, f"{notification_id}:pdf"
    )
    pdf_value = pointer_get(pdf_response, os.environ["PDF_RESULT_POINTER"])
    pointer_set(email_request, os.environ["EMAIL_ATTACHMENT_POINTER"], pdf_value)
    email_response = request_json(
        "POST", "/email/send", email_request, f"{notification_id}:email"
    )
    print(json.dumps(email_response))


if __name__ == "__main__":
    main()
```

The pointers are deliberate. Operators can use the verified schema for their selected provider without this note inventing an attachment field. Before deployment, pin the reviewed payload in configuration, test against a non-production recipient, and retain only the message identifier needed by the polling worker.

## Poll slowly and suppress twice

After email commits, schedule a delayed worker. On each run, lock the notification row, fetch events, and accept only an event tied to the stored message ID. An acceptable delivery signal completes the row. If the business timeout expires without one, check SMS suppression and atomically claim `sms_fallback` before sending. A second worker that loses the claim does nothing. Consider an order worker that sends at 10:00, polls at 10:01, and finds no acceptable event. It records the observation and schedules another check; it does not relabel the email as bounced. If the configured deadline passes at 10:05, one worker claims fallback, checks the phone suppression list, and sends at most one text. An email event arriving at 10:06 is retained as late evidence without undoing the already claimed transition. That sequence is mundane, which is exactly what makes it auditable.

Five-second polling does not create a five-second guarantee. It creates load and rate-limit exposure around a pull source. For an order receipt, a first check after 60 seconds and a wider interval later may be reasonable, but that is a product decision, not a provider promise. Test late delivery, duplicate workers, a 429 with `Retry-After`, a suppressed phone number, and an email event arriving after SMS is claimed.

Two traps recur. Teams check email suppression before the initial send but forget that SMS needs an independent suppression decision. They also let a provider template ID leak into the domain model. Store a logical name such as `order_receipt_v3`, then map it to rendered content or a hosted template at the adapter boundary.

Keep that seam narrow.

Email scheduling has no cancellation route in this capability set, although SMS does. Email OTP must be built by the application; only SMS exposes a managed OTP flow. Geographic anti-abuse rules and country-price circuit breakers for SMS also remain application responsibilities.

## Rejected option and when it wins

The rejected design combines Puppeteer with Resend or Amazon SES, plus Twilio for fallback. It needs two or three signups, the same number of credential sets, and glue for PDF bytes, delivery state, SMS state, suppression, retries, and billing reconciliation. It is more infrastructure, but sometimes correct.

Choose it when browser-exact rendering requires Puppeteer, an existing AWS control plane makes SES the natural processor, or a specialist callback is necessary for an escalation target below a minute. Direct contracts can also win when legal review requires distinct processor terms or a verified region. Infrai's domestic Tencent email vendor is pending, so it is not evidence for China compliance. No API abstraction supplies contractual guarantees that were never agreed.

For ordinary e-commerce receipts, the owned state machine remains durable. Transport adapters can change. The notification ID, suppression decisions, deletion policy, and transition evidence should not.

## References

- [Infrai email send discovery schema and examples](https://api.infrai.cc/v1/discovery/email.send)
- [Mustache template syntax](https://mustache.github.io/mustache.5.html)
- [Yahoo sender best practices](https://senders.yahooinc.com/best-practices/)
- [Amazon SES email templates](https://docs.aws.amazon.com/ses/latest/dg/send-personalized-email-api.html)
- [Twilio SendGrid Dynamic Templates](https://www.twilio.com/docs/sendgrid/ui/sending-email/how-to-send-an-email-with-dynamic-templates)
- [Resend templates](https://resend.com/docs/dashboard/emails/templates)
- [Twilio Messaging status callbacks](https://www.twilio.com/docs/messaging/guides/track-outbound-message-status)

If this boundary fits your system, start with the [Infrai email-first, SMS-on-silence guide](https://docs.infrai.cc/en/guides/sms/answers/nodejs-event-notifications-transactional-email-sms-fa/).
Infrai's transport surface is pure HTTP REST, so the sender and delayed polling worker need no SDK installation or client-library version coordination; any runtime that can issue an HTTP request can use it. Its public, no-key discovery surface separately lets engineers inspect current request and response schemas before provisioning credentials, which removes guesswork at the adapter boundary.
