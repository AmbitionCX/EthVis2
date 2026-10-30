package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"ethvis2/internal/archive"
	"ethvis2/internal/config"
	"ethvis2/internal/domain"
	"ethvis2/internal/storage/clickhouse"
	"ethvis2/internal/storage/postgres"
	"fmt"
	"os"
	"path/filepath"
)

type Selection struct {
	Task       string `json:"task_id"`
	Attempt    string `json:"attempt_id"`
	Generation uint64 `json:"generation,string"`
}
type Publication struct {
	ID            string      `json:"publication_id"`
	ManifestHash  string      `json:"manifest_hash"`
	SelectionHash string      `json:"selection_hash"`
	Attempts      []Selection `json:"attempts"`
}
type Report struct {
	Run             string `json:"run_id"`
	Profile         string `json:"profile"`
	Publication     string `json:"publication_id"`
	BlocksComplete  bool   `json:"blocks_complete"`
	VotesComplete   bool   `json:"votes_complete"`
	ProfileComplete bool   `json:"profile_complete"`
	Blocks          uint64 `json:"core_blocks,string"`
	Aggregates      uint64 `json:"aggregates,string"`
	Members         uint64 `json:"member_inclusions,string"`
	Rewards         string `json:"rewards"`
	BLS             string `json:"bls_verification"`
}

func publicationPath(c config.Config, m domain.Manifest) string {
	return filepath.Join(c.RunsDir, m.ID, "publication.json")
}
func snapshot(ctx context.Context, p *postgres.Control, m domain.Manifest) (Publication, error) {
	pub := Publication{ManifestHash: m.Hash()}
	rows, e := p.Pool.Query(ctx, "SELECT id,attempt_id,generation,status,kind FROM ethvis_tasks WHERE run_id=$1 ORDER BY id", m.ID)
	if e != nil {
		return pub, e
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var s Selection
		var status, kind string
		if e = rows.Scan(&s.Task, &s.Attempt, &s.Generation, &status, &kind); e != nil {
			return pub, e
		}
		if status != "complete" {
			return pub, fmt.Errorf("required task %s is %s", s.Task, status)
		}
		counts[kind]++
		pub.Attempts = append(pub.Attempts, s)
	}
	if e = rows.Err(); e != nil {
		return pub, e
	}
	if counts["block_raw"] != m.Target || counts["block_decode"] != m.Target || counts["slots"] != 1 {
		return pub, errors.New("required block task coverage mismatch")
	}
	if m.Profile == domain.BlocksVotes {
		if counts["attestation_map"] != m.Target {
			return pub, errors.New("required mapping task coverage mismatch")
		}
		var sealed bool
		if e = p.Pool.QueryRow(ctx, "SELECT discovery_complete FROM ethvis_runs WHERE id=$1", m.ID).Scan(&sealed); e != nil || !sealed {
			return pub, errors.New("committee discovery not complete")
		}
	} else if counts["attestation_map"]+counts["committees"] > 0 {
		return pub, errors.New("unexpected vote tasks in blocks profile")
	}
	for kind := range counts {
		switch kind {
		case "block_raw", "block_decode", "slots", "committees", "attestation_map":
		default:
			return pub, errors.New("unexpected task kind")
		}
	}
	b, _ := json.Marshal(pub.Attempts)
	h := sha256.Sum256(b)
	pub.SelectionHash = hex.EncodeToString(h[:])
	id := sha256.Sum256([]byte(pub.ManifestHash + pub.SelectionHash))
	pub.ID = hex.EncodeToString(id[:])
	return pub, nil
}
func checkSelection(ctx context.Context, ch *clickhouse.ClickHouse, m domain.Manifest, pub Publication) error {
	for start := 0; start < len(pub.Attempts); start += 500 {
		var tuples []string
		for _, s := range pub.Attempts[start:min(start+500, len(pub.Attempts))] {
			tuples = append(tuples, "("+clickhouse.Quote(s.Task)+","+clickhouse.Quote(s.Attempt)+")")
		}
		query := "SELECT count() FROM " + ch.Config.Database + ".published_attempts FINAL WHERE run_id=" + clickhouse.Quote(m.ID) + " AND (task_id,attempt_id) IN (" + join(tuples) + ") FORMAT TabSeparated"
		b, e := ch.Request(ctx, query, nil)
		if e != nil {
			return e
		}
		if string(bytes.TrimSpace(b)) != fmt.Sprint(len(tuples)) {
			return errors.New("selected CH attempts differ from PG")
		}
	}
	return nil
}
func join(s []string) string {
	var b bytes.Buffer
	for i, v := range s {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(v)
	}
	return b.String()
}
func Seal(ctx context.Context, c config.Config, m domain.Manifest, p *postgres.Control) error {
	pub, e := snapshot(ctx, p, m)
	if e != nil {
		return e
	}
	ch := clickhouse.NewCH(c)
	if e = checkSelection(ctx, ch, m, pub); e != nil {
		return e
	}
	attempts, _ := json.Marshal(pub.Attempts)
	row, _ := json.Marshal(map[string]string{"run_id": m.ID, "manifest_hash": pub.ManifestHash, "selection_hash": pub.SelectionHash, "attempts_json": string(attempts)})
	if _, e = ch.Request(ctx, "INSERT INTO "+c.Database+".sealed_publications FORMAT JSONEachRow", bytes.NewReader(row)); e != nil {
		return e
	}
	return archive.AtomicJSON(publicationPath(c, m), pub)
}
func Verify(ctx context.Context, c config.Config, m domain.Manifest) (Report, error) {
	p, e := postgres.OpenControl(ctx, c)
	if e != nil {
		return Report{}, e
	}
	defer p.Close()
	maintenance, e := p.Maintenance(ctx)
	if e != nil {
		return Report{}, e
	}
	defer maintenance()
	unlock, e := p.Lock(ctx, m.ID)
	if e != nil {
		return Report{}, e
	}
	defer unlock()
	return verifyLocked(ctx, c, m, p)
}
func verifyLocked(ctx context.Context, c config.Config, m domain.Manifest, p *postgres.Control) (Report, error) {
	report := Report{Run: m.ID, Profile: m.Profile, Rewards: "not_requested", BLS: "not_performed"}
	if e := ValidateManifest(m); e != nil {
		return report, e
	}
	pub, e := snapshot(ctx, p, m)
	if e != nil {
		return report, e
	}
	var disk Publication
	if e = loadPublication(c, m, &disk); e != nil {
		return report, e
	}
	if pub.ID != disk.ID {
		return report, errors.New("publication not sealed for current attempts")
	}
	ch := clickhouse.NewCH(c)
	if e = ch.CheckSchema(ctx); e != nil {
		return report, e
	}
	if e = checkSelection(ctx, ch, m, pub); e != nil {
		return report, e
	}
	hash, e := ch.Request(ctx, "SELECT selection_hash FROM "+c.Database+".sealed_publications FINAL WHERE run_id="+clickhouse.Quote(m.ID)+" FORMAT TabSeparated", nil)
	if e != nil || string(bytes.TrimSpace(hash)) != pub.SelectionHash {
		return report, errors.New("CH publication seal differs")
	}
	db := c.Database
	where := " WHERE run_id=" + clickhouse.Quote(m.ID)
	count := func(table, extra string) (uint64, error) {
		b, e := ch.Request(ctx, "SELECT count() FROM "+db+"."+table+"_published"+where+extra+" FORMAT TabSeparated", nil)
		if e != nil {
			return 0, e
		}
		var n uint64
		_, e = fmt.Sscan(string(b), &n)
		return n, e
	}
	report.Blocks, e = count("beacon_blocks", "")
	if e != nil {
		return report, e
	}
	slots, e := count("slot_coverage", "")
	if e != nil {
		return report, e
	}
	if report.Blocks != uint64(m.Target) || slots != m.Blocks[len(m.Blocks)-1].Slot-m.Blocks[0].Slot+1 {
		return report, errors.New("block/slot coverage mismatch")
	}
	for start := 0; start < len(m.Blocks); start += 500 {
		var tuples []string
		for _, b := range m.Blocks[start:min(start+500, len(m.Blocks))] {
			tuples = append(tuples, "("+clickhouse.Quote(b.Root)+","+fmt.Sprint(b.Slot)+")")
		}
		data, e := ch.Request(ctx, "SELECT uniqExact(tuple(block_root,slot)) FROM "+db+".beacon_blocks_published"+where+" AND (block_root,slot) IN ("+join(tuples)+") FORMAT TabSeparated", nil)
		if e != nil || string(bytes.TrimSpace(data)) != fmt.Sprint(len(tuples)) {
			return report, errors.New("frozen block identities missing")
		}
	}
	report.BlocksComplete = true
	report.Aggregates, e = count("attestation_inclusions", "")
	if e != nil {
		return report, e
	}
	if m.Profile == domain.BlocksVotes {
		maps, e := count("attestation_mapping_coverage", " AND row_id='block'")
		if e != nil || maps != uint64(m.Target) {
			return report, errors.New("block mapping coverage missing")
		}
		coverage, e := count("attestation_mapping_coverage", " AND row_id!='block'")
		if e != nil || coverage != report.Aggregates {
			return report, errors.New("aggregate mapping coverage missing")
		}
		report.Members, e = count("validator_attestation_inclusions", "")
		if e != nil {
			return report, e
		}
		q := "SELECT count() FROM (SELECT block_root,attestation_index,participant_count FROM " + db + ".attestation_inclusions_published" + where + ") a LEFT JOIN (SELECT block_root,attestation_index,count() AS n,uniqExact(validator_index) AS unique_n FROM " + db + ".validator_attestation_inclusions_published" + where + " GROUP BY block_root,attestation_index) v USING(block_root,attestation_index) WHERE a.participant_count!=v.n OR v.n!=v.unique_n FORMAT TabSeparated"
		raw, e := ch.Request(ctx, q, nil)
		if e != nil || string(bytes.TrimSpace(raw)) != "0" {
			return report, errors.New("per-aggregate member count/uniqueness mismatch")
		}
		total, e := ch.Request(ctx, "SELECT sum(participant_count) FROM "+db+".attestation_inclusions_published"+where+" FORMAT TabSeparated", nil)
		if e != nil || string(bytes.TrimSpace(total)) != fmt.Sprint(report.Members) {
			return report, errors.New("orphan/extra member inclusions")
		}
		report.VotesComplete = true
	}
	report.Publication = pub.ID
	report.ProfileComplete = report.BlocksComplete && (m.Profile == domain.Blocks || report.VotesComplete)
	if e = archive.AtomicJSON(filepath.Join(c.RunsDir, m.ID, "report.json"), report); e != nil {
		return report, e
	}
	return report, p.MarkRun(ctx, m.ID, "verified")
}
func loadPublication(c config.Config, m domain.Manifest, p *Publication) error {
	b, e := os.ReadFile(publicationPath(c, m))
	if e != nil {
		return e
	}
	return json.Unmarshal(b, p)
}
