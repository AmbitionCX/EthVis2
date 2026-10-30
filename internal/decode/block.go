package decode

import (
	"encoding/json"
	"errors"
	"ethvis2/internal/domain"
	"fmt"
)

func ParseBlock(raw []byte, ref domain.BlockRef, m domain.Manifest) (domain.Block, []domain.Attestation, error) {
	var b domain.Block
	if e := json.Unmarshal(raw, &b); e != nil {
		return b, nil, e
	}
	b.Original = append(json.RawMessage(nil), raw...)
	msg := b.Data.Message
	slot, e := Number(msg.Slot)
	if e != nil || slot != ref.Slot || msg.Parent != ref.Parent || msg.State != ref.State || b.Optimistic {
		return b, nil, errors.New("block/header identity mismatch or optimistic")
	}
	if b.Version != m.Fork(slot) {
		return b, nil, fmt.Errorf("fork mismatch: got %s expected %s", b.Version, m.Fork(slot))
	}
	if _, e = Hex(b.Data.Signature, 96); e != nil {
		return b, nil, e
	}
	if _, e = Number(msg.Proposer); e != nil {
		return b, nil, e
	}
	var ats []domain.Attestation
	v, ok := msg.Body["attestations"]
	if !ok || string(v) == "null" {
		return b, nil, errors.New("missing attestations array")
	}
	if e = json.Unmarshal(v, &ats); e != nil {
		return b, nil, e
	}
	for _, a := range ats {
		if _, e = Aggregate(a, b.Version, m, slot); e != nil {
			return b, nil, e
		}
	}
	return b, ats, nil
}
func BlockRows(b domain.Block, ats []domain.Attestation, m domain.Manifest, base domain.Row, emit func(domain.Row) error) error {
	add := func(table, id string, v any) error {
		r := base
		r.Dataset = table
		r.ID = id
		p, e := json.Marshal(v)
		if e != nil {
			return e
		}
		r.Payload = string(p)
		return emit(r)
	}
	original := any(b)
	if len(b.Original) > 0 {
		original = b.Original
	}
	if e := add("beacon_blocks", "block", original); e != nil {
		return e
	}
	for kind, raw := range b.Data.Message.Body {
		if kind == "attestations" {
			var originals []json.RawMessage
			if e := json.Unmarshal(raw, &originals); e != nil {
				return e
			}
			for i, a := range ats {
				r, e := Aggregate(a, b.Version, m, base.Slot)
				if e != nil {
					return e
				}
				r.Dataset = "attestation_inclusions"
				r.ID = fmt.Sprint(i)
				r.AttestationIndex = uint64(i)
				r.Payload = string(originals[i])
				if e = emit(r); e != nil {
					return e
				}
			}
			continue
		}
		if kind == "execution_payload" && string(raw) != "null" {
			if e := add("execution_payloads", "payload", raw); e != nil {
				return e
			}
			var p map[string]json.RawMessage
			if e := json.Unmarshal(raw, &p); e != nil {
				return e
			}
			for _, x := range []struct{ key, table string }{{"transactions", "execution_transactions"}, {"withdrawals", "withdrawals"}} {
				if v, ok := p[x.key]; ok {
					var arr []json.RawMessage
					if e := json.Unmarshal(v, &arr); e != nil {
						return e
					}
					for i, item := range arr {
						if e := add(x.table, fmt.Sprint(i), item); e != nil {
							return e
						}
					}
				}
			}
			continue
		}
		if kind == "execution_requests" {
			var req map[string][]json.RawMessage
			if e := json.Unmarshal(raw, &req); e != nil {
				return e
			}
			for name, arr := range req {
				for i, v := range arr {
					if e := add("block_operations", fmt.Sprintf("execution_requests_%s:%d", name, i), v); e != nil {
						return e
					}
				}
			}
			continue
		}
		if len(raw) > 0 && raw[0] == '[' {
			var arr []json.RawMessage
			if e := json.Unmarshal(raw, &arr); e != nil {
				return e
			}
			for i, v := range arr {
				if e := add("block_operations", fmt.Sprintf("%s:%d", kind, i), v); e != nil {
					return e
				}
			}
		} else {
			if e := add("block_operations", kind, raw); e != nil {
				return e
			}
		}
	}
	return nil
}
