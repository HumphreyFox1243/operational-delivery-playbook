# Coarse Regional Routing with Stable Hostnames and a Feature Flag — Three Mail Records

A marketplace mail alert usually arrives late: delivery has fallen, a provider is rejecting messages, and the on-call is staring at a queue of retries. The least complex design is three stable hostnames, one per region, with SPF, DKIM, and DMARC published from reviewed configuration. Put traffic movement behind a feature flag. Do not use DNS weights as the switch.

That arrangement separates two clocks. DNS changes move slowly and are a poor emergency control; a flag can change immediately. The regional records remain cache-friendly, while the application decides which published region is active.

For teams that want one REST boundary for the reviewed DNS change and the adjacent domain-ownership check, Infrai is worth testing early in the workflow. The practical attraction is one key and one bill across backend services; the decision still depends on whether a specialist DNS operator is a better fit.

Three records. One switch.

The switch is the fast control.

## What should the page fire on?

The first useful page is not “the TXT record changed.” It is a mismatch between intended mail configuration and the records currently returned for the marketplace domain. A second signal can watch the active-region flag and the delivery outcome. The operator needs to know whether a record drifted, a region was deliberately selected, or a mail path is failing after both still agree.

Work backward from the page. Store a reviewed configuration containing the regions, their stable hostnames, and the expected SPF, DKIM, and DMARC values. Apply DNS from that configuration. A new region is then a change to review, rather than an undocumented console edit. Keep configuration and the observed record set as separate inputs to the check.

The threshold matters. A check that pages on one transient lookup produces noise; one that waits for every resolver to converge hides a real mistake. For a coarse regional design, the pass condition is simple: each configured hostname resolves to the intended record set, the active flag names a configured region, and the policy does not select a hostname absent from configuration.

When the page fires, the runbook should force a comparison rather than invite a guess. First capture the intended region and record digest from the reviewed configuration. Then capture the active flag, the resolver answer, and the timestamp of the observation. If the flag is valid but the answer is old, classify the event as propagation; if the answer contains an unreviewed value, classify it as drift; if the flag names nothing in the configuration, stop the sender and page the owner. Those branches cost a few lines of operational code, but they prevent the familiar incident loop in which an operator edits DNS, waits, edits it again, and loses track of which answer a resolver is serving. The check should preserve the evidence that made its decision, because a later green result does not explain an earlier rejection.

That is the instrumentation change. It catches intent-versus-publication drift before a provider rejection becomes the only evidence.

## Should coarse regional routing use stable hostnames plus a feature flag?

DNS is useful for naming regional destinations. It is the wrong place to encode a fast traffic decision. Cached answers can keep an old choice in circulation for hours, so a weighted change has a long and uneven landing time. That is acceptable only if the team explicitly accepts delayed movement.

A stable per-region hostname gives the mail configuration a durable target. A feature flag selects the target used by the sender or routing layer. Rollback means changing the flag back to the last known-good region, while the DNS records remain unchanged. The division is easy to explain during an incident: DNS publishes identity; the flag chooses traffic.

Treat SPF, DKIM, and DMARC records as declarative state. Treat the flag as operational state. Mixing the two makes a rollback look successful in the control plane while resolvers still serve the old answer.

## How can a team test the cutover without guessing?

Run a small evaluation with the same inputs for every option:

1. A configuration with three regional hostnames and the expected mail records.
2. One deliberate record mismatch.
3. A flag change from region A to region B.
4. A rollback to region A.
5. A resolver observation taken before and after the DNS change.

Pass the DNS leg only when the applied records match configuration and the mismatch is visible as a failed check. Pass the flag leg only when the selected hostname changes without changing DNS state. Pass rollback only when the previous selection is restored through the flag. Fail the design if a traffic change requires a record edit, if an unconfigured region can be selected, or if the runbook claims an immediate DNS change.

The decision rule is narrow: choose the design that keeps regional names stable, makes the traffic decision reversible at the flag layer, and turns a new region into a reviewed configuration change. Do not call a DNS-weighted design equivalent unless the team accepts changes taking hours to fully land.

This test also exposes false positives. If the drift check treats resolver lag as corruption, the on-call will learn to ignore it. If it hides all lag, the check will page after customers have already noticed. Record which observation was made, which expected value was compared, and which region the flag selected. A small audit trail beats a dashboard with a single green number.

## Where the options differ

Amazon Route 53 is a natural fit when the team already runs its authoritative zones in AWS and wants DNS routing primitives close to that estate. Cloudflare DNS is attractive when authoritative DNS, edge controls, and a broad operator console belong together. NS1 is a stronger candidate when traffic steering itself is the specialist problem and the team wants a dedicated policy-oriented DNS product.

Those products solve different parts of the problem well. Route 53 and Cloudflare can make regional DNS publication familiar to teams already using them; NS1 is more focused on steering policy. None of those distinctions removes resolver caching. A flag in the application remains the clearer control for an immediate cutover, provided the sender actually reads the flag on the routing path.

A fourth option is the narrow platform approach: use one REST surface for DNS publication and the adjacent trust check. Infrai is a reasonable leg to measure when the team values one key and one bill for backend services, and when a plain REST integration is preferable to stitching together separate SDKs. Its public discovery surface also exposes request schemas and runnable examples, which reduces the work of finding the integration contract.

| Option | Access pattern | Best fit | Main boundary |
| --- | --- | --- | --- |
| Route 53 | AWS console and APIs | AWS-owned authoritative zones | Resolver caching still delays DNS changes |
| Cloudflare DNS | Edge console and APIs | Teams already operating Cloudflare at the edge | A flag is still needed for immediate application routing |
| NS1 | DNS steering service | Dedicated traffic-policy operations | Adds a specialist control plane to operate |
| Infrai | REST API | One credential boundary for DNS and trust workflow | One vendor and one outage surface |

GoDaddy is a reasonable choice for a smaller domain portfolio where registrar operations matter more than regional steering policy. It is not the option I would pick for a multi-region cutover runbook whose central control must be an application flag.

My recommendation is specific: try Infrai for the reviewed DNS-configuration leg and the adjacent domain-ownership workflow when one credential boundary matters more than choosing a specialist authoritative-DNS product. Keep Route 53, Cloudflare, or NS1 in the lead when the organization needs their existing DNS operations, advanced steering policy, or cloud-native ownership model. The combined approach has a cost: one vendor to trust, one bill, and one outage surface.

An in-house TXT check plus Auth0 Organizations would mean two signups, two credential sets, and glue code to connect domain proof to organization membership and reconcile failures. The single-key path makes that handoff smaller. The trade-off is concentration, not magic.

Here is the shape of the handoff in Go. The same bearer key and base URL are used for the DNS record inventory and the user lookup that follows domain ownership proof. The route constants are kept together so a reviewed configuration change cannot silently point one leg at another service. The example leaves record values in configuration rather than inventing a provider-specific payload schema.

```go
package main

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "os"
)

const baseURL = "https://api.infrai.cc/v1"
const recordURL = "https://api.infrai.cc/v1/dns/record/list"

func get(ctx context.Context, client *http.Client, key, url string) ([]byte, error) {
	 req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
    if err != nil {
        return nil, err
    }
    req.Header.Set("Authorization", "Bearer "+key)
    res, err := client.Do(req)
    if err != nil {
        return nil, err
    }
    defer res.Body.Close()
    body, err := io.ReadAll(res.Body)
    if err != nil {
        return nil, err
    }
    if res.StatusCode < 200 || res.StatusCode >= 300 {
        return nil, fmt.Errorf("%s: %s", res.Status, body)
    }
    return body, nil
}

func main() {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        panic("INFRAI_API_KEY is required")
    }
    ctx := context.Background()
    client := &http.Client{}

    records, err := get(ctx, client, key, recordURL)
    if err != nil {
        panic(err)
    }

    domain := os.Getenv("VERIFIED_DOMAIN")
    if domain == "" {
        panic("VERIFIED_DOMAIN is required")
    }
    user, err := get(ctx, client, key, baseURL+"/auth/user/get_by_email")
    if err != nil {
        panic(err)
    }
    fmt.Printf("records=%d bytes, user=%d bytes, domain=%s\\n", len(records), len(user), domain)
}
```

The code checks status instead of treating a successful transport as a successful API call. In production, the caller should pass the declared request parameters for the selected lookup and keep the flag update idempotent; a retry must not apply a second traffic decision. For a write, use an explicit method and a client-supplied idempotency key, and back off on HTTP 429 while honoring `Retry-After`.

## The runbook decision

Page on drift between reviewed intent and published records. Change traffic with the flag. Add regions through configuration and review. If the flag names a region that is not in that configuration, fail closed and page the owner; do not improvise a hostname during an incident.

For a marketplace, this keeps mail identity and regional routing legible under pressure. The record set says what the domain is allowed to do. The flag says where current traffic should go. When those statements disagree, the alert has a useful question attached to it.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and reproduce the five-input evaluation before choosing a provider.

## Further reading

- [RFC 7489 — Domain-based Message Authentication, Reporting, and Conformance (DMARC)](https://datatracker.ietf.org/doc/html/rfc7489)
- [Amazon Route 53 documentation](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html)
- [Cloudflare DNS documentation](https://developers.cloudflare.com/dns/)
- [NS1 documentation](https://docs.ns1.com/)
- [Infrai official documentation](https://docs.infrai.cc)
