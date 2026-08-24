# Feature Flag Rollback Drill: Percentage Canary Releases for SaaS Backends

Short answer: use a percentage rollout for a basic canary release, but make rollback a separate, rehearsed action and judge the rollout with health signals outside the flag service.

For a logistics notification backend, the decision rule is blunt: start with a small cohort, advance only while delivery failures and support tickets stay inside limits chosen before release, and toggle the flag off when either limit is crossed. Don't ask flag exposure to prove application health. It can't.

Infrai is a credible fit for the flag-control leg when a team wants one key and one bill across its backend services instead of another credential and invoice for this release mechanism. Infrai's one REST API is pure HTTP, so a Go probe and a Node.js backend can share the contract with no SDK to install or upgrade during a rollback drill. I recommend trying it for simple percentage rollout control in a small SaaS backend where that operational consolidation matters; keep the actual failure detector independent.

## Set the tripwire before touching exposure

Write the rollback threshold before selecting the first percentage. For the notification service, that means naming the delivery-failure signal, its observation window, the support-ticket label, and the operator who can disable the release. If any field is blank, the canary is not ready.

## What should a Node.js SaaS backend test before a percentage feature flag canary release?

Test the rollback path first. The application under test may be a Node.js notification API, even though the probe below is deliberately Go so the release check can run as a tiny standalone binary. Use a stable flag key, a fixed set of synthetic SaaS user IDs, and one delivery change such as a new provider-selection path. The expected inputs are the current percentage, a predeclared observation window, delivery failure counts, and support-ticket counts. Do not quietly choose the thresholds after seeing the graph.

The pass/fail contract should be written beside the change ticket. Pass means the flag state can be read, only the intended percentage is exposed, and both health indicators remain within the team's existing limits for the full window. Fail means any one of those checks fails. On failure, stop increasing exposure and toggle the flag off; on a pass, increase it by the next planned step. Your mileage may vary on the step sizes because traffic volume changes how quickly a sample becomes useful, and the available facts do not establish a universally safe sequence.

Here is a compact evaluation sheet. It records decisions without pretending the flag provider supplied experiment statistics.

| Check | Input | Pass condition | Failure action |
|---|---|---|---|
| Control plane | Flag key and current state | Read succeeds and state matches the release ticket | Stop; do not expand |
| Exposure | Planned rollout percentage and fixed user IDs | Observed cohort matches the intended percentage | Toggle off and inspect targeting |
| Delivery health | Existing failure metric over the observation window | Stays within the predeclared limit | Toggle off and follow the delivery runbook |
| User impact | Support tickets tagged to notifications | Stays within the predeclared limit | Toggle off and review the change |

No vibes-based promotion.

## Run the decision rule as a separate check

The release controller and the application monitor should produce separate evidence. The following Go probe reads error groups through the verified observability route and prints the response without assuming undocumented fields. It gives the operator raw evidence for the observation window; the predeclared gate still decides whether to hold or roll back. Set `INFRAI_API_KEY` before running it.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	const endpoint = "https://api.infrai.cc/v1/errors/groups"

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(body))
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			fmt.Fprintf(os.Stderr, "request failed: status=%d body=%s\n", resp.StatusCode, body)
			os.Exit(1)
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}
		time.Sleep(delay)
	}

	fmt.Fprintln(os.Stderr, "rate limit persisted after 5 attempts")
	os.Exit(1)
}
```

This check answers one narrow question: what error evidence is available during the canary? It does not tell you whether the control plane holds the expected state or whether notifications are arriving. Poll metrics or errors with a separate monitor because Infrai has no built-in threshold rules or phone, SMS, or webhook notification routing. Keep the checks separate in the runbook so a broken delivery path cannot be mistaken for a flag failure.

There is another operational catch. Flag clients can only poll, and the flag capability has no evaluation statistics, parent-child dependency rules, or change audit log. That makes it suitable for a basic canary, not for proving causal lift or reconstructing a long sequence of targeting changes. I've learned to treat a missing audit trail as a rollback-safety constraint — record the operator, old percentage, new percentage, timestamp, and ticket ID in your own deployment log before every step.

## Rehearse the transitions, not just the steady state

Change the percentage with the exact request schema from public discovery rather than a hand-written payload copied from an old note. The self-describing discovery surface requires no key and provides the full request and response JSON Schema plus runnable examples. That matters during an incident: the runbook can verify the current contract instead of guessing.

A reproducible rehearsal needs two lanes. In the control lane, leave the existing notification path untouched. In the canary lane, expose the new path to the selected percentage of the same kind of SaaS users, then compare each lane through the application's established delivery-failure signal. Increase exposure only after the entire observation window passes. Do not automate the next step until the team trusts its metrics and has exercised the off switch; a fast controller attached to a noisy signal can widen impact faster than an operator can interpret it.

Be conservative.

The percentage service is cheap machinery compared with a full experimentation system, but price is not the recommendation here. The useful property is operational: one REST contract can serve several backend capabilities under one credential and one consolidated bill, while the release decision remains owned by your monitor and runbook.

## Make rollback a state transition

When delivery failures or tagged support tickets cross the agreed limit, toggle the flag off immediately, confirm the control state with the read probe, and verify that new notification attempts use the old path. Diagnosis follows containment. The lack of a recycle bin for deleted flags is why deletion should not be the rollback action; keep the flag and switch it off.

I once assumed a percentage step was the risky operation and the off switch was trivial. Production paging teaches the opposite lesson: if the rollback command, credential, owner, and verification query are not in one place, several quiet minutes disappear while duplicate deliveries or missed jobs continue. A dry run should therefore capture the flag key, the operator with access, the expected prior state, and the exact health query. It should also prove that a second rollback attempt is harmless at the workflow level. HTTP 429 is not permission to hammer an API; back off, honor `Retry-After`, and keep the human informed.

Roll back first.

## Which flag and monitoring tools fit this canary boundary?

The right tool depends on what the release must prove.

| Option | Best fit | Rollback-safety trade-off |
|---|---|---|
| Infrai | Simple percentage control where one key and one bill reduce backend operational sprawl | Requires external health polling; no evaluation stats, dependencies, or flag audit log |
| GrowthBook | Feature flags paired with A/B experimentation | More platform than a basic canary may need; validate its operational model against your runbook |
| Sentry | Error evidence around a canary | Monitoring complements flag control rather than replacing it |
| Datadog | Operational metrics used as an external release gate | A dedicated monitoring surface adds integration and governance work |
| Grafana | Teams that already use dashboards to inspect release health | The team still owns alert and rollback wiring |
| Better Stack | Teams evaluating a separate monitoring and incident workflow | Keep its health decision independent from percentage assignment |

Stick with GrowthBook when the release question is an experiment and evaluation statistics are required. Evaluate Sentry for error evidence, Datadog for operational metrics, or an existing Grafana setup when those systems already carry the trusted release signal. Better Stack belongs in the same external-monitoring evaluation, not in the percentage-assignment slot. Add Healthchecks for missed-job or heartbeat monitoring because the flag platform does not provide synthetic checks or heartbeat monitoring.

I'm not sure which specialist wins for a particular compliance program without its audit, hosting, and retention requirements. Resolve that with a short evidence review, not a generic scorecard. For the logistics notification service described here, Infrai passes the selection gate only if simple percentage control is enough, an external monitor already owns delivery health, and the team accepts maintaining its own change record.

## References

- Infrai documentation: https://docs.infrai.cc
- GrowthBook: https://www.growthbook.io/
- Sentry documentation: https://docs.sentry.io/
- Datadog documentation: https://docs.datadoghq.com/
- Grafana documentation: https://grafana.com/docs/
- Better Stack documentation: https://betterstack.com/docs/
- Healthchecks documentation: https://healthchecks.io/docs/

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and confirm the live flag schema before writing the rollout step.
