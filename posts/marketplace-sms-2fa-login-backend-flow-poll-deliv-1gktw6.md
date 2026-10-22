# Marketplace SMS 2FA Login Backend Flow: Poll Delivery Status Before Retrying

TL;DR: For a marketplace signup, treat the verification link as a short-lived workflow with three independently recorded facts: the request was accepted, the message was handed to a downstream carrier, and the user completed verification. Start with a single asynchronous send and a bounded status poll only for unresolved attempts. Page on an abnormal share of unresolved or failed deliveries, not on every individual send. That keeps integration effort small while preserving enough evidence to distinguish a bad destination, a provider-side delay, and a broken callback path.

The least complex design is an adapter around a generic messaging interface, backed by one attempt record and an idempotency key. The signup request should return after the attempt is accepted for dispatch; it should not wait for a carrier receipt. A later status update changes the attempt state, and the user can request one controlled resend.

A page is a symptom, not a delivery mechanism. The operational question is whether the system can explain what happened to a particular verification attempt without sending a second message by accident.

## The page that arrives too late

The on-call view usually starts with a support report: a new seller entered a phone number, saw a successful signup response, and never received the verification link. The dashboard shows a healthy request rate. The application logs show no exception. The page fires only after the account funnel has already lost the user.

Work backward from that page. A successful HTTP response proves that the application accepted a request. It does not prove delivery, and delivery does not prove that the link was opened. Collapsing those facts into `sent` makes the alert look quiet until the business result is already bad.

I initially treated a missing receipt as an immediate send failure. That created noisy incidents during ordinary downstream latency and encouraged retries at exactly the wrong time. The useful correction was to give each attempt a deadline and to record the last known state, timestamp, and reason code separately.

For a marketplace, the minimum record can look like this:

```go
type VerificationAttempt struct {
	ID              string
	SignupID        string
	DestinationHash string
	IdempotencyKey  string
	State           string // accepted, delivered, failed, expired
	Reason          string
	CreatedAt       time.Time
	UpdatedAt       time.Time
	ExpiresAt       time.Time
}
```

Do not store the verification token in an operational log. Store a hash or opaque attempt identifier, enforce a short expiry, and make the verification operation safe to repeat. OWASP recommends rate limiting, consistent responses, and controls against excessive guesses for reset and one-time-secret flows; those controls belong in this path too.

## How should a backend handle SMS 2FA login polling?

The clean boundary is a state machine, not a boolean. `accepted` means the messaging service accepted the request for processing. `delivered` means a downstream delivery notification says the destination was reached. `failed` means a terminal failure was reported. `expired` means the attempt's observation window ended without a usable terminal result. Those states should not be inferred from the signup response.

A callback is preferable when the integration can provide authenticated, retry-safe notifications. Polling is a practical fallback for an unresolved attempt, but it needs a ceiling. Polling forever turns an uncertain delivery into permanent background work; polling aggressively turns a temporary delay into load and duplicate operator noise.

This flow also fits a Node or Express login service, but the framework is not the reliability boundary. The same rule applies to an OTP send: handle acceptance, delivery status, and verification as separate facts. A framework-level success response cannot turn an unobserved SMS into a delivered login code.

The worker below shows the important decisions without tying them to a particular provider. It claims one attempt, asks for its current status, and records a transition only if the state is newer. In production, `Repository` would also enforce a lease so two workers cannot process the same attempt concurrently.

```go
type StatusClient interface {
	Lookup(ctx context.Context, key string) (state string, reason string, observedAt time.Time, err error)
}

type Repository interface {
	Claim(ctx context.Context, id string, now time.Time) (VerificationAttempt, bool, error)
	Advance(ctx context.Context, id, state, reason string, observedAt time.Time) error
}

func Observe(ctx context.Context, repo Repository, client StatusClient, id string, now time.Time) error {
	attempt, ok, err := repo.Claim(ctx, id, now)
	if err != nil || !ok {
		return err
	}

	state, reason, observedAt, err := client.Lookup(ctx, attempt.IdempotencyKey)
	if err != nil {
		return err // retain the attempt for the next bounded observation
	}
	if observedAt.Before(attempt.UpdatedAt) {
		return nil
	}
	return repo.Advance(ctx, attempt.ID, state, reason, observedAt)
}
```

The adapter should also reject a second send for the same signup and purpose while the first attempt is still unresolved. A user-facing resend can create a new attempt only after a deliberate cooldown, with the old attempt marked superseded. This is the small detail that prevents a carrier delay from becoming two links, two support tickets, and an avoidable account lockout.

## How do you set an alert without paging on latency?

Start with a signal that matches the failure users experience: the ratio of attempts that end in `failed` or `expired` within a fixed observation window. Pair it with a count floor. A percentage from two attempts is not an incident; a percentage from thousands may be.

A useful first policy is to evaluate five-minute and one-hour windows, keep the threshold in configuration, and require the same condition in two consecutive evaluations before paging. The exact threshold belongs to the marketplace's baseline and destination mix. The important distinction is between a warning metric and a page: a warning can identify an emerging carrier or country problem, while a page should indicate that signup verification is broadly impaired.

Instrument these dimensions:

- acceptance rate and request latency at the application boundary;
- terminal delivery outcomes by country or destination class, without logging the raw number;
- age of unresolved attempts, including the oldest attempt;
- verification completion after delivery;
- callback authentication failures, lookup errors, and duplicate transition attempts.

Alert on a broken observation path separately from a delivery problem. If callbacks stop arriving, the delivery metric may look merely unresolved. That is a control-plane failure and deserves its own signal.

False positives have a cost. They train the on-call to mute the alert, and then a real destination outage gets treated as another transient. Before changing the threshold, inspect the distribution of attempt age and the reason codes. A threshold should be adjusted because the expected operating envelope changed, not because one noisy afternoon was uncomfortable.

## The integration decision is an evidence decision

For a small team, integration effort is more than the number of endpoints. Ask whether the interface has stable event identifiers, idempotent submission, a documented status vocabulary, authenticated callbacks, replay behavior, and enough reason detail to support a user-facing retry policy. A simple interface with weak evidence can demand more application code later.

Keep the application contract narrow:

```go
type Messenger interface {
	SendVerification(ctx context.Context, destination, link, key string) error
}

// The delivery observer owns status translation; signup owns only acceptance.
type DeliveryEvent struct {
	Key        string
	State      string
	Reason     string
	ObservedAt time.Time
}
```

Then test the boundaries that tend to page people: a timeout after acceptance, a duplicated callback, an out-of-order callback, a failed resend, a status lookup that remains unavailable, and a link that expires while delivery is still unresolved. Use a fake clock and deterministic event fixtures. The test should prove that retries do not send twice and that an old event cannot move a terminal attempt backward.

Yahoo's sender guidance is aimed at email, but its operational lesson generalizes: identity, authentication, complaint handling, and reputation affect whether a communication channel remains usable. For SMS or other channels, keep the same separation between application acceptance and downstream outcome, then map the channel's own delivery semantics into the internal state machine.

The runbook should answer three questions in under a minute: how many new attempts are unresolved, how old the oldest is, and whether terminal failures cluster by destination class or reason. If those answers are unavailable, the next integration feature should be observability, not another retry path.

The result is intentionally modest: one durable attempt record, one idempotency boundary, one bounded observer, and alerts based on outcomes rather than request success. It gives a marketplace team a clear escalation path without pretending that a backend response can prove a message reached a person.

There is a real limitation. A bounded poll cannot prove that a handset displayed a message, and a delivery receipt cannot prove that a user controls the number. This design is a poor fit for a channel that offers no trustworthy status signal or for a high-assurance login that requires a stronger authenticator; in those cases, accepting weaker evidence is the wrong integration trade-off.

## Further reading

- OWASP, “Forgot Password Cheat Sheet”: https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- Yahoo, “Sender Best Practices and Requirements”: https://senders.yahooinc.com/best-practices/
- RFC 9457, “Problem Details for HTTP APIs”: https://www.rfc-editor.org/rfc/rfc9457
