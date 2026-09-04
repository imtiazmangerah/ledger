package ctrl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func newAuditAlignmentController(t *testing.T, appliedIndex uint64, sequences ...uint64) (*DefaultController, *readstore.Store) {
	t.Helper()

	logger := logging.NopZap()
	meter := noop.NewMeterProvider().Meter("test")
	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	batch := store.OpenWriteSession()
	for _, sequence := range sequences {
		key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).PutUint64(sequence).Build()
		require.NoError(t, batch.SetProto(key, &auditpb.AuditEntry{
			Sequence: sequence,
			Outcome:  &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}},
			Ledgers:  []string{"main"},
		}))
	}
	require.NoError(t, state.SetAppliedIndex(batch, appliedIndex))
	require.NoError(t, batch.Commit())

	rs, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = rs.Close() })

	return NewDefaultController(nil, store, logger, attributes.New(), rs, nil, nil, nil, meter), rs
}

func auditSequenceFilter(minimum uint64) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Audit{Audit: &commonpb.AuditCondition{
		Field: commonpb.AuditField_AUDIT_FIELD_SEQUENCE,
		Condition: &commonpb.AuditCondition_UintCond{UintCond: &commonpb.UintCondition{
			Min: &minimum,
		}},
	}}}
}

func auditLedgerFilter(ledger string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Audit{Audit: &commonpb.AuditCondition{
		Field: commonpb.AuditField_AUDIT_FIELD_LEDGER,
		Condition: &commonpb.AuditCondition_StringCond{StringCond: &commonpb.StringCondition{
			Value: &commonpb.StringCondition_Hardcoded{Hardcoded: ledger},
		}},
	}}}
}

func collectAuditSequences(t *testing.T, c cursor.Cursor[*auditpb.AuditEntry]) []uint64 {
	t.Helper()

	entries, err := cursor.Collect(c)
	require.NoError(t, err)
	sequences := make([]uint64, len(entries))
	for index, entry := range entries {
		sequences[index] = entry.GetSequence()
	}

	return sequences
}

func TestListAuditEntriesOnlyWaitsWhenFilterUsesAuditProjection(t *testing.T) {
	t.Parallel()

	ctrl, rs := newAuditAlignmentController(t, 12, 1, 2)
	rs.SetAuditProjectionState(true, false)

	for name, filter := range map[string]*commonpb.QueryFilter{
		"unfiltered":    nil,
		"sequence-only": auditSequenceFilter(2),
	} {
		t.Run(name, func(t *testing.T) {
			c, err := ctrl.ListAuditEntries(context.Background(), 10, 0, filter, false)
			require.NoError(t, err, "a query independent of the disabled projection must remain available")
			got := collectAuditSequences(t, c)
			if filter == nil {
				require.Equal(t, []uint64{1, 2}, got)
			} else {
				require.Equal(t, []uint64{2}, got)
			}
		})
	}

	_, err := ctrl.ListAuditEntries(context.Background(), 10, 0, auditLedgerFilter("main"), false)
	require.ErrorContains(t, err, "audit (disabled)",
		"a query that depends on a disabled projection must fail explicitly")

	rs.SetAuditProjectionState(false, true)
	_, err = ctrl.ListAuditEntries(context.Background(), 10, 0, auditLedgerFilter("main"), false)
	require.ErrorContains(t, err, "audit (rebuilding)",
		"a query that depends on a rebuilding projection must fail explicitly")
}

func TestListAuditEntriesIndexedFilterHonorsCancellation(t *testing.T) {
	t.Parallel()

	ctrl, _ := newAuditAlignmentController(t, 12, 1)
	ctx, cancel := context.WithCancel(query.WithReadBarrierHorizon(context.Background(), 12))
	cancel()

	_, err := ctrl.ListAuditEntries(ctx, 10, 0, auditLedgerFilter("main"), false)
	require.ErrorIs(t, err, context.Canceled,
		"an unavailable required projection must not silently return an incomplete result")
}

func TestListAuditEntriesUsesAlignedAuditSnapshotAndMainHorizon(t *testing.T) {
	t.Parallel()

	ctrl, rs := newAuditAlignmentController(t, 12, 1)
	batch := rs.NewBatch()
	kb := dal.NewKeyBuilder()
	require.NoError(t, batch.SetBytes(readstore.AuditIndexStringKey(kb, readstore.AuditFieldLedger, "main", 1), nil))
	// This row is ahead of the fixed main snapshot and must be horizon-trimmed.
	require.NoError(t, batch.SetBytes(readstore.AuditIndexStringKey(kb, readstore.AuditFieldLedger, "main", 2), nil))
	require.NoError(t, rs.WriteAuditProgress(batch, 2))
	require.NoError(t, rs.WriteAuditRaftProgress(batch, 12))
	require.NoError(t, batch.Commit())

	ctx := query.WithReadBarrierHorizon(context.Background(), 12)
	c, err := ctrl.ListAuditEntries(ctx, 10, 0, auditLedgerFilter("main"), false)
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, collectAuditSequences(t, c),
		"audit candidates ahead of H must be trimmed before main-store materialization")
}
