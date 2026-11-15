# Patient Mail Delivery and Signed Asset Access Through Custom Domain DNS

Publish SPF, DKIM, and DMARC for the patient-message sending domain, then verify mail authentication and private asset access independently before moving traffic onto a customer hostname. The deciding constraint is evidence: successful DNS resolution cannot establish that a message authenticates, reaches an inbox, or grants the right person access to an attachment.

TL;DR: Treat mail authentication, customer-domain routing, and signed asset URLs as separate release checks. DMARC evaluates the relationship between the visible sender domain and authenticated mail identities. It does not authorize downloads. For a healthtech reminder containing a private document link, require both an observed authentication result and an explicit asset authorization decision.

## What does a successful delivery actually prove?

Start the runbook with the evidence you need to close it. An appointment reminder can reach a receiving mail system while its document link fails, and a working document link says nothing about whether the sender passed DMARC. Record those outcomes separately.

RFC 7489 defines DMARC around the domain in the message's RFC5322.From field. A passing result requires a successful, aligned SPF result or a successful, aligned DKIM result; both are not required. Alignment matters because a successful authentication result for an unrelated domain does not establish the relationship that DMARC checks. The RFC distinguishes relaxed alignment from strict alignment. Under relaxed alignment, organizational domains can match; strict alignment requires the domains to match exactly.

That distinction is operationally useful. Before changing enforcement, inspect which authenticated identity actually aligns for each legitimate sending stream. A queue worker sending reminders and another sending document notifications should each have evidence, even if their visible sender address looks identical. A green dashboard for one stream does not qualify the other.

Keep the proof narrow.

DMARC reports provide authentication evidence. They don't certify inbox placement, prove that a patient read a message, or demonstrate that a download was authorized. For this runbook, maintain a release record with the sending stream, visible sender domain, observed SPF and DKIM identities, alignment outcome, receiving-system disposition, and a separate result for the private link. Use synthetic messages and synthetic documents when testing the complete path. Do not put patient content into the release record.

## Which DNS records let a customer domain serve private assets?

Use an illustrative customer domain such as clinic.example, with a dedicated asset hostname such as documents.clinic.example. These names describe the proposed design, not an existing deployment. Keep the intended mail identities and the asset hostname visible in the change request so a reviewer can see which behavior each DNS change supports.

SPF authenticates an envelope identity, while DKIM associates a signing domain with a message. DMARC connects qualifying authentication to the visible From domain through alignment. Publish the SPF authorization and DKIM verification material required by the actual sending arrangement, then publish the DMARC policy at the appropriate _dmarc name. Do not invent an SPF include or a DKIM public key to fill out a configuration example. Obtain the real values from the sending system and review the resulting identities.

For the asset hostname, make the routing and certificate requirements explicit in the deployment contract. The concrete record type and target depend on that arrangement. Require the serving system to demonstrate the expected hostname and certificate before admitting private requests. There is no universal DNS record value that can safely stand in for those deployment details.

The trade-off is an extra release boundary.

It adds review work, but prevents an asset routing change from being treated as proof that the sending domain authenticates. Keep a record of previous DNS values and the time of each change. Avoid combining a new asset hostname, a new signing identity, and stricter mail enforcement into one unobservable cutover.

RFC 7489 defines three policy values: p=none, p=quarantine, and p=reject. A monitoring policy is useful while identifying legitimate senders, but it does not request the same treatment as rejection. Move toward enforcement only when the evidence accounts for the streams that must continue working. Receiver policy still matters; a published DMARC request is not a delivery guarantee.

## Make queued links survive retries without broadening access

The asset side needs its own authorization contract.

In this proposed design, a signed link carries a tenant identifier, an asset identifier, an expiry, and a signature over an unambiguous representation of those fields and the intended hostname. Require authorization before returning private bytes, including on a cache hit. Have the implementation review demonstrate that behavior; a successful uncached request is insufficient evidence.

A signature check alone should not be the release criterion. Also require the expected customer hostname, the tenant-to-asset relationship, an acceptable expiry, and any revocation rule the application promises. Decide whether possession of the link is sufficient or a patient session must also authorize access. That choice changes what a forwarded message can expose.

The scheduling hazard is link lifetime. A link created when a job enters the queue may have little useful lifetime left when a delayed worker sends the message. Prefer issuing it near the actual send attempt. Make the lifetime a documented clinical workflow decision, with room for expected queue delay and recipient action, rather than an arbitrary constant copied from another service.

Retries need a separate identity.

Use a stable notification identifier to reconcile repeated work; do not treat a newly issued URL as evidence that the underlying notification is new. A send attempt whose outcome is unknown deserves reconciliation before an automatic resend. Fresh signatures cannot resolve an ambiguous delivery result. Consider this hypothetical sequence during review: the worker submits a reminder, loses the acknowledgement, and retries after generating another signed URL. The second signature may be valid, but it tells the operator nothing about whether the first message was accepted. Require the retry policy to address that uncertainty explicitly. Record the logical notification identifier across attempts, the evidence available from the sending system, and the decision to retry or hold. This design accepts reconciliation work because an extra patient notification can create confusion even when both links enforce the intended access policy.

The following Go example is a release gate for the proposed design. Its inputs represent independently collected checks; it does not implement DNS inspection, DMARC evaluation, signature verification, or proof of inbox placement.

```go
package release

import "errors"

type Evidence struct {
    MailAlignmentObserved bool
    CustomerHostVerified  bool
    AuthorizedReadPassed  bool
    CrossTenantReadDenied bool
    ExpiredLinkDenied     bool
    CachedReadChecked     bool
    RetryReconciled       bool
}

func Check(e Evidence) error {
    if !e.MailAlignmentObserved {
        return errors.New("collect aligned mail authentication evidence")
    }
    if !e.CustomerHostVerified {
        return errors.New("verify the customer hostname and certificate")
    }
    if !e.AuthorizedReadPassed || !e.CrossTenantReadDenied ||
        !e.ExpiredLinkDenied || !e.CachedReadChecked {
        return errors.New("complete private asset authorization checks")
    }
    if !e.RetryReconciled {
        return errors.New("reconcile the notification retry outcome")
    }
    return nil
}
```

Keep these inputs false until the associated evidence exists. In particular, an HTTP success response cannot establish cross-tenant denial or an expiry boundary. Exercise those cases directly with synthetic assets.

## Verify the cutover against the recipient workflow

Test the whole reminder, from queued job to received message to document retrieval. Capture the authentication result at the receiver and inspect the aligned identity. Then open the asset through the customer hostname using the intended recipient context.

Run the negative cases too: an altered asset identifier, the wrong tenant, a different hostname, and a link past its expiry. Repeat the authorization checks after the asset has been requested once, so the test covers the cache path. The required outcome is denial without private content. Keep access tokens and signed query strings out of routine logs; use notification and asset identifiers to correlate test evidence.

For mail, review aggregate reporting to find authentication patterns that a single seed message could miss. RFC 7489 also describes failure reporting and its privacy considerations. Review report destinations and data handling before enabling more detailed reporting in a healthtech workflow. Authentication telemetry should have an owner and a review cadence, not just a mailbox that accumulates unread reports.

Classify the failure before changing anything.

An alignment failure sends the investigation toward sender identities and DNS policy. A private-link denial sends it toward the asset authorization contract. A delayed reminder sends it toward queue age and dispatch history. This classification keeps a resend from becoming the default response to every patient-facing symptom.

## Roll back the failed boundary and preserve evidence

Before cutover, define who can pause reminder dispatch, restore prior DNS values, and reverse an enforcement change. If authentication regresses after a policy change, assess the affected legitimate streams and restore the reviewed policy when appropriate. A monitoring policy reduces the requested enforcement; document that trade-off and retain the evidence needed to correct alignment.

If the asset deployment fails its access checks, pause affected notifications while restoring the last verified serving configuration.

Never make private documents public to clear a queue.

Reconcile pending and uncertain send attempts before replay, because restoring service does not establish which messages were already accepted.

Close the change only when mail authentication and private asset access each have evidence under the customer domain. The operational goal is a reminder the receiving system can evaluate and a document the intended recipient can access under the chosen policy. Keep those two promises visible in the runbook.

## References

- RFC 7489 — Domain-based Message Authentication, Reporting, and Conformance (DMARC), including identifier alignment, policy, reporting, and privacy considerations: https://datatracker.ietf.org/doc/html/rfc7489
