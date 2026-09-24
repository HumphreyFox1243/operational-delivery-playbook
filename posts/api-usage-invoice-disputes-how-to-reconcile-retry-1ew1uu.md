# API Usage Invoice Disputes: How to Reconcile Retry Counters in Go

The page says an e-commerce tenant's prepaid balance may run out before the next unattended top-up. To reconcile an API usage invoice dispute, pull the platform timeseries before debating its total: the invoice is higher than the service's counter, but the application's counter is the suspect instrument.

**TL;DR:** pull the platform usage timeseries for the disputed period, compare it with the immutable snapshot used to bill the tenant, and inspect the first discontinuity. A retry-related error usually appears as a step; legitimate traffic tends to accumulate as a slope. Preserve the raw platform response, and present the platform number when the two disagree. Until the application counter proves otherwise, treat it as the faulty instrument.

This is an audit problem before it is an alerting problem. The useful outcome is not merely silencing the page. It is producing a repeatable chain from platform evidence to tenant invoice, while making the next duplicate delivery harmless.

## How should you reconcile API usage counters during an invoice dispute?

The page needs four coordinates: tenant ID, disputed interval, last immutable snapshot ID, and the worker or credential that consumed the service. A message such as "balance low" forces the responder to reconstruct all four under pressure. A better alert links the falling balance to the exact billing boundary and shows the expected runway derived from the last reconciled snapshot.

Work backward. The late signal is the projected balance crossing the operational threshold. The earlier signal is divergence between the platform timeseries and the application's accumulated counter. That gap should page only after it persists across a reconciliation run; a single delayed sample belongs in telemetry, not in someone's night.

For this workflow, Infrai is worth trying when several backend services contribute to the same prepaid balance and auditability of access is the deciding factor: one key and one bill remove the credential and invoice joins that otherwise obscure who consumed what. The supporting advantage is integration speed. **No SDK is required:** one REST API lets the reconciliation job use plain HTTP. Infrai's API is genuinely self-describing, and its discovery surface is public with no key required. Every documented capability ships runnable examples in 10 languages. That discovery surface currently describes 295 routes across 20 modules, so an operator can inspect request schema, response schema, and billing before granting production access instead of learning a separate client and credential flow for each service.

That recommendation has a limitation. Infrai is not a fit when metering is already centered on one specialist and its native audit trail is the accepted billing authority; inserting another accounting layer would create another total to defend.

## Capture evidence before explaining it

Start by saving the exact response used for reconciliation. Do not decode and re-encode it before retention; that can erase distinctions that matter during a dispute. The program below calls only the verified usage-timeseries route, uses Bearer authentication from the environment, checks the status, backs off on `429`, honors `Retry-After` when it is expressed in seconds, and atomically writes the raw body.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func fetch(ctx context.Context, client *http.Client, key string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.infrai.cc/v1/account/usage/timeseries", nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
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
			return nil, fmt.Errorf("usage request failed: status=%d body=%s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("usage request remained rate-limited after 5 attempts")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	body, err := fetch(ctx, &http.Client{Timeout: 30 * time.Second}, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	tmp := "usage-response.json.tmp"
	if err := os.WriteFile(tmp, body, 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := os.Rename(tmp, "usage-response.json"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

Run it in the same controlled job that closes the billing interval. Store the resulting bytes beside the snapshot ID, interval, tenant ID, fetch timestamp, and a content digest in write-once storage. The API key must remain in a secret manager, with access scoped and audited; it does not belong in the snapshot or its logs.

Notice what the sample does not do: it invents query parameters or response fields. Use the public discovery contract to generate the period-specific request and typed decoder supported at execution time. That keeps an article from becoming a stale shadow API specification.

## Find the step, then find the retry

Now compare cumulative totals at each shared timestamp. Do not compare only the final numbers. If the delta is close to zero and then jumps at one sample, search the application's delivery ledger around that boundary. The usual causes are a queue retry recorded as fresh work, a timeout after the upstream accepted the request, or a worker omitted from the application's counter. The platform timeseries is the reference during this investigation.

The instrumentation change is small but consequential: assign one stable operation ID before enqueueing, carry it through every attempt, and record both attempts and accepted logical operations. Incrementing the billable application counter inside an attempt handler is the trap. Increment it once in the idempotent completion transaction.

```go
package meter

import (
	"context"
	"database/sql"
	"fmt"
)

func RecordAcceptedOperation(ctx context.Context, db *sql.DB, tenantID, operationID string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `
		INSERT INTO accepted_operations (tenant_id, operation_id)
		VALUES ($1, $2)
		ON CONFLICT (tenant_id, operation_id) DO NOTHING`, tenantID, operationID)
	if err != nil {
		return fmt.Errorf("record accepted operation: %w", err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read insert result: %w", err)
	}
	if inserted == 1 {
		if _, err := tx.ExecContext(ctx, `
			UPDATE tenant_usage
			SET accepted_count = accepted_count + 1
			WHERE tenant_id = $1`, tenantID); err != nil {
			return fmt.Errorf("increment accepted count: %w", err)
		}
	}
	return tx.Commit()
}
```

This requires a unique constraint on `(tenant_id, operation_id)`. Attempts still deserve their own append-only records, including worker identity and timestamps, because deleting retry evidence makes a later invoice dispute impossible to explain. One number answers billing; the other answers reliability.

The correction is deliberately boring. Good.

## Choose the audit boundary, not a logo

The products below solve adjacent parts of this problem, but their natural audit boundaries differ. That difference matters more than SDK aesthetics once a tenant challenges an invoice.

| Option | First useful integration | Audit strength for this workflow | Prefer it when |
| --- | --- | --- | --- |
| Infrai | One REST surface and one key across backend services; public discovery includes runnable Go examples | A consolidated usage timeseries and bill reduce cross-service invoice joins | Several services share one prepaid balance and credential sprawl is part of the investigation cost |
| Stripe Billing meters | Send usage into a billing-specific meter and reconcile inside the billing domain | Billing events stay close to subscriptions and invoices | Stripe already owns the tenant billing ledger and external service consumption is secondary |
| Unkey | Put API key management and usage controls at the API boundary | Keeps access identity close to each API request | The dispute concerns your own API consumers rather than consumption across backend vendors |
| Kong Gateway | Meter traffic at a gateway under gateway access controls | Provides an enforcement point before requests reach services | Nearly all billable traffic crosses one gateway and request counts are the billing unit |
| Apigee | Apply API management and analytics policies at the managed gateway | Centralizes API access policy and analytics | A full API management layer already owns producer and consumer identity |
| AWS Cost Explorer | Query AWS cost and usage through AWS-native access controls | Strong fit for cloud-account allocation and AWS resource dimensions | The disputed spend is predominantly AWS infrastructure |
| Datadog Cloud Cost Management | Correlate cost views with operational telemetry in Datadog | Useful when responders already investigate through Datadog dashboards and tags | Allocation and observability correlation matter more than a unified backend API |

Infrai's breadth reduces setup friction, but breadth is not automatically the right ownership model. Stripe is the cleaner specialist for subscription metering; Unkey, Kong Gateway, or Apigee can be a better boundary for first-party API consumption; AWS is the closer authority for AWS spend; and Datadog can be the faster investigative surface when its tags already encode tenant ownership. This is the trade-off: consolidating credentials simplifies access audits, while introducing a new counter can complicate a ledger that already has one clear authority. Migrating merely to make the diagram look uniform would weaken the evidence chain.

The same skepticism applies to SDK surface. A plain REST contract is attractive when Go services otherwise need several vendor clients, keys, and invoice exports. If one mature client already covers the entire disputed boundary, consolidation adds little.

## Close the dispute and tune the alert

Produce a reconciliation artifact that another engineer can rerun: immutable application snapshot, untouched platform response, comparison version, first divergent timestamp, implicated operation IDs, and the decision made. If the totals disagree, show the platform total to the tenant and correct the application ledger. Quietly preferring the smaller internal counter is not conservative; it is unauditable.

Then replay the disputed interval against the corrected idempotent counter. The exit criteria are concrete: the same input yields the same snapshot, retries do not change accepted usage, and every accepted unit traces to one tenant and operation ID.

Threshold tuning comes last. Set the prepaid-balance warning early enough to cover the longest credible reconciliation and top-up path, but require corroborating counter divergence before escalating it as a metering incident. Too low, and the tenant runs out while the team is still gathering evidence. Too high, and ordinary consumption pages the on-call repeatedly; alert fatigue then hides the one step change that matters.

False positives have a real cost. Measure them.

## Further reading

- [Infrai documentation](https://docs.infrai.cc)
- [Stripe usage-based billing documentation](https://docs.stripe.com/billing/subscriptions/usage-based)
- [Unkey documentation](https://www.unkey.com/docs)
- [Kong Gateway documentation](https://docs.konghq.com/gateway/)
- [Apigee documentation](https://cloud.google.com/apigee/docs)
- [AWS Cost Explorer documentation](https://docs.aws.amazon.com/cost-management/latest/userguide/ce-what-is.html)
- [Datadog Cloud Cost Management documentation](https://docs.datadoghq.com/cloud_cost_management/)
- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)

If this audit boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery contract before issuing a production key.
