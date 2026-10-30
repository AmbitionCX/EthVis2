package decode

import (
	"encoding/json"
	"ethvis2/internal/domain"
	"fmt"
	"strings"
	"testing"
)

func testRoot(n int) string { return fmt.Sprintf("0x%064x", n) }
func sample() domain.Attestation {
	return domain.Attestation{Bits: "0x35", CommitteeBits: "0x05", Signature: "0x" + strings.Repeat("11", 96), Data: domain.AttestationData{Slot: "8", Index: "0", Head: testRoot(8), Source: domain.Checkpoint{Epoch: "1", Root: testRoot(4)}, Target: domain.Checkpoint{Epoch: "2", Root: testRoot(8)}}}
}
func TestElectraMultiCommitteeAndPositions(t *testing.T) {
	m := domain.Manifest{SlotsPerEpoch: 4, MaxCommittees: 8}
	a := sample()
	base, e := Aggregate(a, "electra", m, 10)
	if e != nil {
		t.Fatal(e)
	}
	cs := CommitteeSet{Key(8, 0): {0, 9007199254740993}, Key(8, 2): {12, 13, 14}}
	var got []domain.Row
	if e = Map(a, "electra", m, cs, base, func(r domain.Row) error { got = append(got, r); return nil }); e != nil {
		t.Fatal(e)
	}
	want := []uint64{0, 12, 14}
	for i, row := range got {
		if row.Validator != want[i] {
			t.Fatalf("member %d: %+v", i, row)
		}
	}
	if len(got) != 3 || got[1].Committee != 2 || got[1].Position != 0 || got[1].AggregationPosition != 2 || got[2].Position != 2 {
		t.Fatalf("wrong positions: %+v", got)
	}
	a.Bits = "0x03"
	if e = Map(a, "electra", m, cs, base, func(domain.Row) error { return nil }); e == nil {
		t.Fatal("accepted wrong bit length")
	}
	delete(cs, Key(8, 2))
	if e = Map(sample(), "electra", m, cs, base, func(domain.Row) error { return nil }); e == nil {
		t.Fatal("accepted missing committee")
	}
}
func TestBitsAndLegacy(t *testing.T) {
	for _, s := range []string{"0x", "0x00", "0x0100", "0x0", "zz"} {
		if _, e := Bitlist(s); e == nil {
			t.Fatalf("accepted %s", s)
		}
	}
	bits, e := Bitlist("0x0101")
	if e != nil || len(bits) != 8 || !bits[0] || bits[7] {
		t.Fatal(bits, e)
	}
	if _, e = Bitvector("0x80", 7); e == nil {
		t.Fatal("accepted padding")
	}
	a := sample()
	a.CommitteeBits = ""
	a.Data.Index = "2"
	a.Bits = "0x0a"
	m := domain.Manifest{SlotsPerEpoch: 4, MaxCommittees: 8}
	base, e := Aggregate(a, "deneb", m, 10)
	if e != nil {
		t.Fatal(e)
	}
	var got []domain.Row
	if e = Map(a, "deneb", m, CommitteeSet{Key(8, 2): {1, 2, 3}}, base, func(r domain.Row) error { got = append(got, r); return nil }); e != nil || len(got) != 1 || got[0].Validator != 2 {
		t.Fatal(got, e)
	}
	if _, e = Selected(a, "unknown", 8); e == nil {
		t.Fatal("unknown fork accepted")
	}
}
func TestCommitteeIntegrityAndDataRoot(t *testing.T) {
	if _, e := Committees([]domain.Committee{{Index: "0", Slot: "8", Validators: []string{"0", "0"}}}, 2, 4, 8); e == nil {
		t.Fatal("duplicate validator accepted")
	}
	if _, e := Number("+1"); e == nil {
		t.Fatal("signed decimal accepted")
	}
	a := sample()
	r, e := DataRoot(a.Data)
	if e != nil || len(r) != 66 {
		t.Fatal(r, e)
	}
	a.Data.Index = "1"
	r2, _ := DataRoot(a.Data)
	if r == r2 {
		t.Fatal("index omitted from SSZ root")
	}
	a.Data.Head = "0x123"
	if _, e = DataRoot(a.Data); e == nil {
		t.Fatal("invalid root accepted")
	}
}
func TestBlockPreservesPayloadUnknownsAndNoRewards(t *testing.T) {
	m := domain.Manifest{SlotsPerEpoch: 4, MaxCommittees: 8, Forks: map[string]string{"phase0": "0", "electra": "2"}}
	ref := domain.BlockRef{Slot: 10, Root: testRoot(10), Parent: testRoot(9), State: testRoot(100)}
	body := map[string]any{"attestations": []domain.Attestation{sample()}, "future_field": map[string]any{"keep": "yes"}, "execution_payload": map[string]any{"transactions": []string{"0x0102"}, "withdrawals": []any{map[string]string{"amount": "18446744073709551615"}}}}
	obj := map[string]any{"version": "electra", "data": map[string]any{"signature": "0x" + strings.Repeat("11", 96), "message": map[string]any{"slot": "10", "proposer_index": "0", "parent_root": ref.Parent, "state_root": ref.State, "body": body}}}
	raw, _ := json.Marshal(obj)
	b, ats, e := ParseBlock(raw, ref, m)
	if e != nil {
		t.Fatal(e)
	}
	counts := map[string]int{}
	if e = BlockRows(b, ats, m, domain.Row{Slot: 10}, func(r domain.Row) error {
		counts[r.Dataset]++
		if strings.Contains(r.Dataset, "reward") {
			t.Fatal("reward dataset")
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if counts["execution_transactions"] != 1 || counts["withdrawals"] != 1 || counts["attestation_inclusions"] != 1 || counts["block_operations"] != 1 {
		t.Fatal(counts)
	}
}
