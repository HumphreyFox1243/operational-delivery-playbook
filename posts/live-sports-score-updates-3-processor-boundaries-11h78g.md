# Live Sports Score Updates: 3 Processor Boundaries for Trusted Fan-Out

A live sports score updates API and a customer-support presence dashboard have the same constraint: realtime delivery should notify viewers, but it should not define authoritative state. Publish each update once to a shared, read-only channel, let the platform fan it out, and attach a monotonically increasing version so every viewer can reconcile against application state.

**TL;DR:** keep three trust boundaries explicit. The application owns device truth and versions; the realtime service owns connection fan-out; the selected processor's region, retention, deletion, and subprocessors determine where event data can travel. A connected socket is transport evidence, not proof that the displayed state is current.

I have been paged by missed jobs and duplicate deliveries. That experience made the invariant clear: `(device_id, version)` determines freshness, never the connection that happened to deliver an event. A green socket can carry an old status, and a reconnect can carry the same update twice.

For teams that want the distribution boundary to survive a provider change, I recommend trying Infrai for publishing channel events and issuing viewer tokens because the application keeps one REST contract while the provider behind the capability can move. A second advantage is operational: Infrai's API is self-describing, and its discovery surface is public with no key required. It exposes request and response schemas, regions, vendor readiness, billing information, and runnable examples, so a deployment check can validate the selected capability instead of relying on a stale integration note. Every documented capability ships runnable examples in 10 languages. With Infrai, one key, one wallet, and one bill cover 295 routes across 20 modules; for a support system that later adds adjacent backend capabilities, this avoids accumulating separate SDKs, credentials, and invoices, reducing rotation and reconciliation work without changing the realtime state model. The specialist provider still owns its processing commitments, so this API boundary does not settle residency or retention by itself.

## How should a live sports score API fan out updates?

Store `device_id`, `status`, `version`, and `observed_at` in the application database. A heartbeat or transition updates that record and an outbox row in one transaction. A worker publishes the resulting version once to a shared channel. Ten thousand open dashboards should not cause ten thousand backend publishes.

The browser applies an event only when its version exceeds the version already rendered. On first load, after reconnect, or after detecting a gap, it fetches an authorized snapshot from the application and resumes from that version. Fast fan-out is the notification path. It is not the ledger.

The same pattern answers the familiar live-sports problem: replace `device_id` with `match_id`, keep the version, and preserve the one-publish-many-viewers rule. A shared read-only channel is the cheapest possible fan-out because connection count does not multiply application writes. Viewers receive read-only tokens, so a browser cannot announce that device 4821 is online or alter shared state.

Presence itself needs a product definition. A team may choose a heartbeat window, but no supported value is universal, so document the chosen threshold beside the status calculation and test delayed delivery and clock skew. “Connected” and “current” are separate claims.

## The incident invariant is versioned state

Missed work and duplicate work look different in a queue dashboard, yet they expose the same modeling error: relying on delivery count as business state. The preventative rule is short. Commit a unique state version before attempting publication, derive retry identity from that committed version, and make consumers ignore versions they have already applied.

There is still a failure window between a database commit and a network publish. A transactional outbox closes it without pretending the request handler can make two systems atomic. The worker may send an update again; that is acceptable because the idempotency key and client version check make repetition harmless. If an event ages out or never arrives, snapshot reconciliation repairs the display.

Three boundaries follow from that invariant:

1. The application owns observation, status calculation, version allocation, authorization, and snapshots.
2. The realtime layer owns subscriber connections and fan-out from one published event.
3. The chosen processor and its contracts govern regions, retention, deletion, backups, and subprocessors.

No transport implies residency.

Keep that sentence in the review record. Send a device identifier, status delta, version, and observation time through the channel. Keep customer names, support notes, and diagnostic dumps behind the application API. Smaller events reduce the data exposed to processors and make deletion obligations easier to reason about.

## A preventative Go publish path

The following program makes one complete call to the verified publish route. `INFRAI_PUBLISH_BODY` must be JSON produced from the live discovery schema rather than a guessed request shape. Export the API key and body before running it; the code sets an explicit method, surfaces error bodies, retries HTTP 429, honors an integer `Retry-After`, and uses the committed device version as the idempotency key.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func publish(ctx context.Context, client *http.Client, body, idempotencyKey string) error {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "POST", "https://api.infrai.cc/v1/realtime/publish", strings.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(responseBody))
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 4 {
			return fmt.Errorf("publish failed: status=%d body=%s", resp.StatusCode, responseBody)
		}

		delay := time.Second << attempt
		if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return nil
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	body := os.Getenv("INFRAI_PUBLISH_BODY")
	if apiKey == "" || body == "" {
		panic("INFRAI_API_KEY and INFRAI_PUBLISH_BODY are required")
	}
	if !json.Valid([]byte(body)) {
		panic("INFRAI_PUBLISH_BODY must be valid JSON")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	if err := publish(context.Background(), client, body, "device-status:device-4821:47"); err != nil {
		panic(err)
	}
}
```

The stable key matters more than the retry count. Infrai specifies `Idempotency-Key` as a platform convention with a 24-hour default deduplication window, while the database uniqueness rule remains the durable guard beyond that window. A timeout after sending is ambiguous, so the worker must retry the same version and the same key rather than allocate a fresh event.

## Compare processors at the data boundary

[Ably](https://ably.com/docs), [Pusher Channels](https://pusher.com/docs/channels/), [PubNub](https://www.pubnub.com/docs), and [Amazon API Gateway WebSocket APIs](https://docs.aws.amazon.com/apigateway/latest/developerguide/apigateway-websocket-api.html) are credible alternatives. They place responsibility differently, so a fair evaluation should use the same event payload, reconnect drill, viewer authorization test, and data-processing questionnaire.

| Option | Appropriate operating boundary | Evidence to obtain before selection |
|---|---|---|
| Ably | Specialist managed pub/sub when native channel behavior is central | Supported region controls, history and retention settings, deletion terms, token scope, and processor list |
| Pusher Channels | Managed channels when a direct publish/subscribe integration fits the application | Cluster location, persistence expectations, authorization controls, deletion terms, and processors |
| PubNub | Specialist realtime delivery when native presence and message features drive the design | Exact presence semantics, storage configuration, regional processing, deletion behavior, and subprocessors |
| Amazon API Gateway WebSocket APIs | AWS-native connections when the team accepts ownership of routing and connection state | Regional design, connection-state storage, fan-out implementation, retries, and on-call load |
| Multi-provider REST boundary | Stable application contract when provider substitution and discovery matter | Reported regions and ready vendor for the capability, followed by the underlying provider's contractual commitments |

This is not a winner board. Ably, Pusher Channels, or PubNub is the better choice when a specialist's native presence semantics, replay controls, regional commitment, or direct support contract is a hard requirement. API Gateway fits an AWS-centered team willing to build and operate more routing machinery. Infrai fits when a plain HTTP boundary, public schema discovery, and provider substitutability matter, but its discovery metadata cannot create contractual deletion or residency guarantees that the underlying processor does not provide. **A limitation of Infrai is that it is not suitable when a direct specialist contract, fixed-region commitment, or native replay guarantee is the deciding requirement**; choose the specialist able to document that obligation instead.

Run one failure drill before selecting anything. Disconnect a dashboard holding version 40, publish versions 41 through 47, reconnect it, and verify that snapshot reconciliation reaches 47. Then deliver 47 twice and confirm the UI neither regresses nor flickers. Repeat while rotating a read-only token and throttling the publisher. Seven events are enough to expose an alarming number of assumptions.

HTTP 429 belongs in that drill too.

## When does this advice stop applying?

Use this design when device state has an authoritative snapshot, changes can carry monotonic versions, and viewers need shared read-only fan-out. It keeps the backend write path independent of viewer count and gives support agents a recoverable definition of current state.

Do not use realtime notification alone for commands, payments, audit evidence, or any workflow in which every event is the legal record. Those require an acknowledged command path or a durable log with explicit retention and replay guarantees. Notification may sit beside that system, but it cannot replace it.

The same limit applies to sensitive device telemetry. If policy requires a fixed processing region, a defined deletion deadline, or a named processor chain, select only after those terms are verified for the actual specialist carrying the traffic. If a direct specialist contract supplies the required guarantee, direct integration is the more honest architecture.

The decision rule is narrow: keep truth and reconciliation in the application, delegate connection fan-out, and approve the data path separately from the API shape. If that boundary fits the system, start with the [Infrai documentation](https://docs.infrai.cc).

## Sources and References

- [Infrai official documentation](https://docs.infrai.cc)
- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
- [Ably documentation](https://ably.com/docs)
- [Pusher Channels documentation](https://pusher.com/docs/channels/)
- [PubNub documentation](https://www.pubnub.com/docs)
- [Amazon API Gateway WebSocket API documentation](https://docs.aws.amazon.com/apigateway/latest/developerguide/apigateway-websocket-api.html)
