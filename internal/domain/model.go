package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

const SchemaVersion = 3
const ParserVersion = "block-v1"
const MappingVersion = "votes-v1"
const Blocks = "blocks"
const BlocksVotes = "blocks_votes"

func ValidProfile(p string) bool { return p == Blocks || p == BlocksVotes }

type BlockRef struct {
	Root   string `json:"root"`
	Parent string `json:"parent"`
	State  string `json:"state"`
	Slot   uint64 `json:"slot,string"`
}
type Manifest struct {
	Version       int               `json:"version"`
	ID            string            `json:"id"`
	Profile       string            `json:"profile"`
	Network       string            `json:"network"`
	AnchorRoot    string            `json:"anchor_root"`
	AnchorEpoch   uint64            `json:"anchor_epoch,string"`
	SlotsPerEpoch uint64            `json:"slots_per_epoch,string"`
	MaxCommittees uint64            `json:"max_committees_per_slot,string"`
	Forks         map[string]string `json:"forks"`
	Blocks        []BlockRef        `json:"blocks"`
	Target        int               `json:"target"`
	Frozen        bool              `json:"frozen"`
	Created       time.Time         `json:"created_at"`
	Raw           []RawRef          `json:"raw"`
}

func (m Manifest) Hash() string {
	b, _ := json.Marshal(m)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func (m Manifest) Fork(slot uint64) string {
	name := "phase0"
	epoch := uint64(0)
	for _, n := range []string{"phase0", "altair", "bellatrix", "capella", "deneb", "electra", "fulu"} {
		value, ok := m.Forks[n]
		e, err := strconv.ParseUint(value, 10, 64)
		if ok && err == nil && e <= slot/m.SlotsPerEpoch && e >= epoch {
			name = n
			epoch = e
		}
	}
	return name
}

type CommitteeInput struct {
	Epoch  uint64 `json:"epoch,string"`
	State  string `json:"state"`
	TaskID string `json:"task_id"`
}
type Task struct {
	ID           string           `json:"id"`
	Kind         string           `json:"kind"`
	Block        *BlockRef        `json:"block,omitempty"`
	Epoch        uint64           `json:"epoch,string"`
	Context      string           `json:"context,omitempty"`
	State        string           `json:"state,omitempty"`
	Raw          RawRef           `json:"raw,omitempty"`
	Inputs       []CommitteeInput `json:"committee_inputs,omitempty"`
	InputAttempt string           `json:"input_attempt,omitempty"`
	Dependencies []string         `json:"dependencies,omitempty"`
	Attempt      string           `json:"-"`
	Generation   uint64           `json:"-"`
}
type Result struct {
	Task Task
	Path string
	Rows uint64
	Refs []RawRef
	Err  error
}
type Checkpoint struct {
	Epoch string `json:"epoch"`
	Root  string `json:"root"`
}
type AttestationData struct {
	Slot   string     `json:"slot"`
	Index  string     `json:"index"`
	Head   string     `json:"beacon_block_root"`
	Source Checkpoint `json:"source"`
	Target Checkpoint `json:"target"`
}
type Attestation struct {
	Bits          string          `json:"aggregation_bits"`
	CommitteeBits string          `json:"committee_bits,omitempty"`
	Signature     string          `json:"signature"`
	Data          AttestationData `json:"data"`
}
type Committee struct {
	Index      string   `json:"index"`
	Slot       string   `json:"slot"`
	Validators []string `json:"validators"`
}
type Block struct {
	Original   json.RawMessage `json:"-"`
	Version    string          `json:"version"`
	Optimistic bool            `json:"execution_optimistic"`
	Data       struct {
		Signature string `json:"signature"`
		Message   struct {
			Slot     string                     `json:"slot"`
			Proposer string                     `json:"proposer_index"`
			Parent   string                     `json:"parent_root"`
			State    string                     `json:"state_root"`
			Body     map[string]json.RawMessage `json:"body"`
		} `json:"message"`
	} `json:"data"`
}

// Identical metadata/typed projection columns make bounded generic chunking possible.
// Payload preserves complete objects; graph queries use typed columns directly.
type Row struct {
	Run                 string `json:"run_id"`
	Task                string `json:"task_id"`
	Attempt             string `json:"attempt_id"`
	Dataset             string `json:"dataset"`
	ID                  string `json:"row_id"`
	Network             string `json:"network"`
	Root                string `json:"block_root"`
	Slot                uint64 `json:"slot,string"`
	State               string `json:"state_root"`
	RawHash             string `json:"raw_sha256"`
	CommitteeHash       string `json:"committee_sha256"`
	Context             string `json:"context_id"`
	InputAttempt        string `json:"input_attempt"`
	Payload             string `json:"payload"`
	PayloadHash         string `json:"payload_sha256"`
	AttestationIndex    uint64 `json:"attestation_index,string"`
	AttestationSlot     uint64 `json:"attestation_slot,string"`
	Epoch               uint64 `json:"epoch,string"`
	Validator           uint64 `json:"validator_index,string"`
	Committee           uint64 `json:"committee_index,string"`
	Position            uint64 `json:"committee_position,string"`
	AggregationPosition uint64 `json:"aggregation_position,string"`
	Head                string `json:"head_root"`
	SourceEpoch         uint64 `json:"source_epoch,string"`
	SourceRoot          string `json:"source_root"`
	TargetEpoch         uint64 `json:"target_epoch,string"`
	TargetRoot          string `json:"target_root"`
	DataRoot            string `json:"attestation_data_root"`
	Participants        uint64 `json:"participant_count,string"`
	BitLength           uint64 `json:"bit_length,string"`
}

var Tables = []string{"beacon_blocks", "block_operations", "execution_payloads", "execution_transactions", "withdrawals", "attestation_inclusions", "beacon_committees", "validator_attestation_inclusions", "slot_coverage", "attestation_mapping_coverage"}

func TableOK(s string) bool {
	for _, t := range Tables {
		if t == s {
			return true
		}
	}
	return false
}
func (r Row) Validate() error {
	if !TableOK(r.Dataset) || r.ID == "" || r.Task == "" || r.Attempt == "" {
		return fmt.Errorf("invalid row identity")
	}
	return nil
}
