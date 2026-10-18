# Node.js Storefront Credential Recovery: Owning Email Templates and SMS Fallback

A password-reset message is a security operation, not a marketing send. **Short answer:** make a short-lived email link the normal recovery path, permit SMS OTP only after an explicit eligibility check, and keep both message templates under the team that owns the recovery state machine. In a US/EU storefront, that choice gives operators one auditable definition of expiry, reuse, localization, and redaction while leaving channel delivery behind narrow adapters.

This matters around an e-commerce order. A shopper who has just paid may need account access to retrieve the receipt, but payment settlement and credential recovery are separate workflows. Never make the receipt job wait for a reset message, and never treat possession of an order number as proof that a caller owns the account.

## What failure are we containing?

The bounded incident pattern is familiar from running scheduled and queued work: a worker times out after handing work to an external system, retries, and produces a second delivery because it cannot tell whether the first attempt crossed the boundary. I treat recovery sends with the same suspicion. The dangerous unit is not `sendEmail()`; it is the transition from an unused recovery challenge to one accepted credential change.

The invariant is small enough for a runbook: one recovery intent has one opaque identifier, a fixed expiry, a capped attempt count, and at most one successful consumption. Delivery may happen more than once. Consumption may not.

Retries lie.

Do not infer delivery from email opens. Apple documents that Mail Privacy Protection can prevent senders from learning whether a recipient opened a message, so an open pixel is unsuitable evidence for advancing recovery state. The useful signals are under your control: challenge created, dispatch accepted, challenge verified, challenge consumed, and credential changed.

Keep the order-receipt queue independent. If payment settles while recovery delivery is degraded, enqueue the receipt using the order event's idempotency key and let account recovery follow its own retry budget. Coupling them turns a channel outage into a fulfillment-support incident.

## Put template ownership beside policy

Template ownership decides who can accidentally change security semantics. If a communications team owns only presentation while the identity team owns variables, expiry copy, destination masking, and action URLs, the split can work. If an external template editor can introduce a second link or render an unescaped field, the recovery boundary has moved outside the code review path.

I use a strict render contract. The template receives display text, a prebuilt HTTPS action URL, expiry text, locale, and support copy. It never receives a reusable bearer token as a free-standing field, raw order data, or a switch that chooses the verification policy. Preview fixtures contain fake values. Production logs contain a template version and intent ID, not the destination or secret.

There are three viable ownership models:

| Model | Useful when | Operational cost |
| --- | --- | --- |
| Identity team owns markup and policy | Recovery changes are rare and tightly reviewed | Slower editorial changes |
| Communications team owns markup behind a typed schema | Localization changes often | Schema validation and approval gates are mandatory |
| Provider-hosted template with version pinning | The provider boundary is already accepted | Export, rollback, and audit evidence must be tested |

My default is the middle model because it separates wording from authorization without pretending they are unrelated. The exception is a small team with no independent communications workflow; there, keeping markup next to recovery code may be the clearer control.

## Should a password reset email have SMS OTP fallback?

SMS should not fire merely because an email worker reported a transient error. That creates two live channels for the same intent, surprises the user, and makes retry storms expensive to reason about. Instead, define eligibility before dispatch: the account has a previously verified phone number, local policy permits its use for security messages, the risk decision allows fallback, and the user explicitly selects the alternate path. Consider the awkward sequence before implementing it: the email transport accepts a request, the worker loses the acknowledgement, and the retry is delayed behind other work. If that delay automatically selects SMS, the shopper can receive an OTP first and an email link later. Both messages look current. The account now has two recovery surfaces, support cannot infer which one the shopper saw, and transport timing has silently become authorization policy. Explicit selection prevents that timing accident.

That is the trade-off.

Consent deserves precise handling in the EU. GDPR Article 7 sets conditions for consent, including demonstrability and the ability to withdraw it. Do not silently reuse marketing consent as the basis for an account-recovery message. Record the applicable purpose and policy decision separately, and have counsel determine the lawful basis for the actual deployment.

The following Go example is intentionally a state transition, not a transport tutorial. A Node.js service should enforce the same database constraints inside a transaction; the language runtime does not supply single-use semantics.

```go
package recovery

import (
	"context"
	"errors"
	"time"
)

type Channel string

const (
	Email Channel = "email_link"
	SMS   Channel = "sms_otp"
)

type Intent struct {
	ID               string
	ExpiresAt        time.Time
	ConsumedAt       *time.Time
	VerifiedPhone    bool
	SMSPolicyAllowed bool
	UserSelectedSMS  bool
}

type Store interface {
	LockIntent(ctx context.Context, id string) (Intent, error)
	RecordDispatch(ctx context.Context, id string, channel Channel, key string) error
}

func SelectAndRecord(ctx context.Context, db Store, id string, now time.Time) (Channel, error) {
	intent, err := db.LockIntent(ctx, id)
	if err != nil {
		return "", err
	}
	if intent.ConsumedAt != nil || !now.Before(intent.ExpiresAt) {
		return "", errors.New("recovery intent unavailable")
	}

	channel := Email
	if intent.UserSelectedSMS && intent.VerifiedPhone && intent.SMSPolicyAllowed {
		channel = SMS
	}

	// A unique constraint on (intent ID, channel) makes worker retries harmless.
	if err := db.RecordDispatch(ctx, intent.ID, channel, intent.ID+":"+string(channel)); err != nil {
		return "", err
	}
	return channel, nil
}
```

The storage layer still carries the hard guarantees: compare expiry against server time, hash stored secrets, lock or atomically update the intent, and reject a second consumption. The dispatch key suppresses duplicate jobs for one channel; it does not make an OTP reusable, and it does not prove that a carrier or mailbox delivered anything.

## How should this be deployed and operated?

Start with deterministic tests around boundaries: one instant before expiry, exactly at expiry, a repeated consumption, an absent verified phone, and two workers selecting a channel concurrently. Then run contract tests against rendered templates. Parse every action URL, confirm the host and scheme, verify that required variables fail closed, and snapshot each supported locale with synthetic data.

Deploy policy and templates by immutable version. A recovery intent records the policy version and template version chosen when it was created, which makes a later support investigation intelligible. Rollback changes which version new intents select; it should not rewrite an outstanding intent halfway through its life.

Page on user impact, not raw provider noise. Useful service-level indicators include the age of the oldest undispatched intent, verification success by channel and locale, duplicate dispatch suppression, terminal expiry count, and the gap between intent creation and successful consumption. Keep email and SMS dimensions separate so one healthy aggregate cannot hide a broken path. Avoid destination addresses, phone numbers, tokens, and full action URLs in logs or metric labels.

The runbook should distinguish four cases: queue backlog, transport rejection, verification failure, and consumption conflict. Each has a different response. Increasing retries may help a temporary transport failure; it makes a malformed template or deterministic policy rejection louder without making it better.

Cost belongs in the capacity plan, not the recovery decision. Set per-account and per-destination rate limits, bound attempts, and forecast channel volume from observed eligible requests. Do not weaken the normal email path just to shift traffic between providers.

## When does this advice not apply?

Do not add SMS fallback when the service has no previously verified number, cannot separate security use from marketing preferences, or cannot protect support agents from social-engineering overrides. A second channel expands the attack and operational surface. Email-only recovery with a documented manual-review path can be the more defensible system.

**The limitation is deliberate:** explicit fallback adds one user step and requires the application to maintain verified-phone and policy state. A team unable to operate those controls should choose a single email channel, not approximate them with automatic failover. Conversely, an environment that mandates a stronger recovery factor should choose that prescribed factor instead of either pattern described here.

No universal channel exists.

The design also changes for accounts protected by stronger recovery factors or for regulated environments that prescribe specific authentication controls. In those cases, the local control framework outranks this channel pattern. The invariant remains useful: delivery is retryable, authorization is explicit, and successful consumption is singular.

For an e-commerce team, template ownership is therefore not a branding detail. It is the boundary that keeps the receipt message, the email recovery link, and the fallback OTP from quietly sharing data or policy they should not share. Own that boundary, rehearse its failure modes, and let transports remain replaceable.

## Sources

- Apple, "Use Mail Privacy Protection on iPhone": https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios
- GDPR Article 7, "Conditions for consent": https://gdpr-info.eu/art-7-gdpr/
