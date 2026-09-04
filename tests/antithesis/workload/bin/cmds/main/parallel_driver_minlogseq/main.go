// Driver for the prepared-query projection-alignment property. After a write
// has been acknowledged, a successful default (linearizable) read must include
// it: ReadIndex fixes a Raft horizon and the query waits only for the read
// projection to certify that same horizon.
//
// Soundness against benign interleavings:
//   - Owned "minseq-" ledger (restricted prefix) + per-run unique query name:
//     no foreign writes, deletes, or query-name collisions.
//   - The probe account is created by the very write whose ack supplies S, so
//     index coverage of S implies the account row exists.
//   - Transient errors (Unavailable, NotFound from lagging metadata) →
//     inconclusive, skip.
package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

const probeAccount = "minseq-probe:main"

func main() {
	internal.RunDriver("parallel_driver_minlogseq", func(ctx context.Context, client servicepb.BucketServiceClient, _ string) {
		r := internal.Rand()

		run := r.Uint64()
		ledger := internal.PrefixMinLogSeq.WithSeed(run)
		if err := internal.CreateLedger(ctx, client, ledger); err != nil {
			return
		}

		queryName := fmt.Sprintf("minseq-q-%d", run)
		details := internal.Details{"ledger": ledger, "queryName": queryName}

		// Prepared query matching the probe account by address prefix.
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_CreatePreparedQuery{
				CreatePreparedQuery: &servicepb.CreatePreparedQueryRequest{
					Ledger: ledger,

					Query: &commonpb.PreparedQuery{
						Name:   queryName,
						Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
						Filter: &commonpb.QueryFilter{
							Filter: &commonpb.QueryFilter_Address{
								Address: &commonpb.AddressMatch{
									Match: &commonpb.AddressMatch_HardcodedPrefix{
										HardcodedPrefix: "minseq-probe:",
									},
								},
							},
						},
					},
				},
			},
		}))
		if err != nil {
			// Ambiguous or transient: without a confirmed query, every later
			// outcome is unattributable — inconclusive.
			return
		}

		// Write the probe transaction; its ack carries the log sequence S.
		resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings: []*commonpb.Posting{{
								Source:      "world",
								Destination: probeAccount,
								Amount:      commonpb.NewUint256FromUint64(1),
								Asset:       "USD/2",
							}},
							Reference: fmt.Sprintf("minseq-%d", run),
							Force:     true,
						},
					}},
				},
			},
		}))
		if err != nil {
			return
		}

		logs := resp.GetLogs()
		if len(logs) == 0 {
			return
		}

		ackedSeq := logs[len(logs)-1].GetSequence()
		details = details.With(internal.Details{"ackedSeq": ackedSeq})

		// A successful default read is linearizable and projection-aligned.
		execResp, err := client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
			Ledger:    ledger,
			QueryName: queryName,
			PageSize:  100,
		})
		if err != nil {
			return
		}
		assert.Reachable("projection-aligned prepared query succeeded", details)

		cursor := execResp.GetCursor()
		if cursor == nil {
			assert.Unreachable("prepared query returned no cursor result", details)

			return
		}

		found := false
		for _, account := range cursor.GetAccountData() {
			if account.GetAddress() == probeAccount {
				found = true

				break
			}
		}

		assert.Always(found,
			"projection-aligned read includes the acknowledged write",
			details.With(internal.Details{"returned": len(cursor.GetAccountData())}))
	})
}
