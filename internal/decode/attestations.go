package decode

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"ethvis2/internal/domain"
	"fmt"
	"strconv"
)

func Number(s string) (uint64, error) {
	if s == "" {
		return 0, errors.New("missing integer")
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, errors.New("invalid unsigned decimal")
		}
	}
	return strconv.ParseUint(s, 10, 64)
}
func hash(a, b [32]byte) [32]byte {
	var x [64]byte
	copy(x[:32], a[:])
	copy(x[32:], b[:])
	return sha256.Sum256(x[:])
}
func DataRoot(d domain.AttestationData) (string, error) {
	var leaves [8][32]byte
	for _, v := range []struct {
		s string
		i int
	}{{d.Slot, 0}, {d.Index, 1}} {
		n, e := Number(v.s)
		if e != nil {
			return "", e
		}
		binary.LittleEndian.PutUint64(leaves[v.i][:8], n)
	}
	b, e := Hex(d.Head, 32)
	if e != nil {
		return "", e
	}
	copy(leaves[2][:], b)
	for j, c := range []domain.Checkpoint{d.Source, d.Target} {
		n, e := Number(c.Epoch)
		if e != nil {
			return "", e
		}
		var x, y [32]byte
		binary.LittleEndian.PutUint64(x[:8], n)
		b, e := Hex(c.Root, 32)
		if e != nil {
			return "", e
		}
		copy(y[:], b)
		leaves[3+j] = hash(x, y)
	}
	r := hash(hash(hash(leaves[0], leaves[1]), hash(leaves[2], leaves[3])), hash(hash(leaves[4], leaves[5]), hash(leaves[6], leaves[7])))
	return "0x" + hex.EncodeToString(r[:]), nil
}
func Selected(a domain.Attestation, version string, max uint64) ([]uint64, error) {
	switch version {
	case "phase0", "altair", "bellatrix", "capella", "deneb":
		if a.CommitteeBits != "" {
			return nil, errors.New("unexpected committee_bits")
		}
		n, e := Number(a.Data.Index)
		if e != nil || n >= max {
			return nil, errors.New("invalid committee index")
		}
		return []uint64{n}, nil
	case "electra", "fulu":
		if a.Data.Index != "0" {
			return nil, errors.New("Electra data.index must be zero")
		}
		x, e := Bitvector(a.CommitteeBits, max)
		if e != nil || len(x) == 0 {
			return nil, errors.New("empty/invalid committee_bits")
		}
		return x, nil
	default:
		return nil, fmt.Errorf("unsupported fork %s", version)
	}
}
func Aggregate(a domain.Attestation, version string, m domain.Manifest, inclusion uint64) (domain.Row, error) {
	var r domain.Row
	slot, e := Number(a.Data.Slot)
	if e != nil {
		return r, e
	}
	target, e := Number(a.Data.Target.Epoch)
	if e != nil || target != slot/m.SlotsPerEpoch {
		return r, errors.New("target epoch/slot mismatch")
	}
	source, e := Number(a.Data.Source.Epoch)
	if e != nil || source > target || slot >= inclusion {
		return r, errors.New("invalid source or inclusion slot")
	}
	if _, e = Hex(a.Signature, 96); e != nil {
		return r, e
	}
	bits, e := Bitlist(a.Bits)
	if e != nil {
		return r, e
	}
	if _, e = Selected(a, version, m.MaxCommittees); e != nil {
		return r, e
	}
	root, e := DataRoot(a.Data)
	if e != nil {
		return r, e
	}
	r = domain.Row{AttestationSlot: slot, Epoch: slot / m.SlotsPerEpoch, Head: a.Data.Head, SourceEpoch: source, SourceRoot: a.Data.Source.Root, TargetEpoch: target, TargetRoot: a.Data.Target.Root, DataRoot: root, BitLength: uint64(len(bits))}
	for _, b := range bits {
		if b {
			r.Participants++
		}
	}
	if r.Participants == 0 {
		return r, errors.New("empty aggregate")
	}
	return r, nil
}

type CommitteeSet map[string][]uint64

func Key(slot, index uint64) string { return fmt.Sprintf("%d:%d", slot, index) }
func Committees(cs []domain.Committee, epoch, slots, max uint64) (CommitteeSet, error) {
	out := CommitteeSet{}
	seenValidators := map[uint64]bool{}
	for _, c := range cs {
		slot, e := Number(c.Slot)
		if e != nil || slot/slots != epoch {
			return nil, errors.New("committee epoch mismatch")
		}
		idx, e := Number(c.Index)
		if e != nil || idx >= max {
			return nil, errors.New("committee index invalid")
		}
		key := Key(slot, idx)
		if _, ok := out[key]; ok || len(c.Validators) == 0 {
			return nil, errors.New("duplicate/empty committee")
		}
		arr := make([]uint64, len(c.Validators))
		for i, v := range c.Validators {
			n, e := Number(v)
			if e != nil || seenValidators[n] {
				return nil, errors.New("invalid/duplicate committee member in epoch")
			}
			seenValidators[n] = true
			arr[i] = n
		}
		out[key] = arr
	}
	if len(out) == 0 {
		return nil, errors.New("empty committee response")
	}
	return out, nil
}
func Map(a domain.Attestation, version string, m domain.Manifest, cs CommitteeSet, base domain.Row, emit func(domain.Row) error) error {
	indices, e := Selected(a, version, m.MaxCommittees)
	if e != nil {
		return e
	}
	bits, e := Bitlist(a.Bits)
	if e != nil {
		return e
	}
	var length int
	for _, idx := range indices {
		members, ok := cs[Key(base.AttestationSlot, idx)]
		if !ok {
			return fmt.Errorf("missing committee slot=%d index=%d", base.AttestationSlot, idx)
		}
		length += len(members)
	}
	if length != len(bits) {
		return fmt.Errorf("aggregation length %d differs from committees %d", len(bits), length)
	}
	seen := map[uint64]bool{}
	offset := 0
	for _, idx := range indices {
		for pos, v := range cs[Key(base.AttestationSlot, idx)] {
			if bits[offset+pos] {
				if seen[v] {
					return errors.New("duplicate mapped validator")
				}
				seen[v] = true
				r := base
				r.Validator = v
				r.Committee = idx
				r.Position = uint64(pos)
				r.AggregationPosition = uint64(offset + pos)
				r.ID = fmt.Sprintf("%d:%d", base.AttestationIndex, v)
				r.Dataset = "validator_attestation_inclusions"
				r.Payload = "{}"
				if e = emit(r); e != nil {
					return e
				}
			}
		}
		offset += len(cs[Key(base.AttestationSlot, idx)])
	}
	if uint64(len(seen)) != base.Participants {
		return errors.New("mapped count mismatch")
	}
	return nil
}
