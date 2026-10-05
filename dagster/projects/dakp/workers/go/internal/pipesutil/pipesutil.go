// Package pipesutil wraps the Dagster Pipes protocol for DAKP-pattern
// extraction workers: session open/close, structured logging, materialization
// reporting, and BLAKE3 content-addressed cache metadata.
//
// Dependency pin: github.com/hupe1980/dagster-pipes-go (protocol version 0.1).
// Swap point: if the upstream lib stalls, reimplement the protocol here
// (context file in $DAGSTER_PIPES_CONTEXT, JSON messages to the file in
// $DAGSTER_PIPES_MESSAGES) without touching callers.
package pipesutil

import (
	"crypto/hex"

	dagsterpipes "github.com/hupe1980/dagster-pipes-go"
	"github.com/zeebo/blake3"
)

// WorkerVersion participates in every cache key. Bump when extraction
// behavior changes so content-addressed artifacts invalidate correctly.
const WorkerVersion = "0.1.0"

// CacheInfo describes a content-addressed artifact decision.
type CacheInfo struct {
	Key         string // BLAKE3(inputs || worker version), hex
	ArtifactPath string // where the output artifact lives
	Hit         bool   // true when the artifact was already present
	InputBytes  int64  // size of the input consumed
}

// HashInput derives a cache key from input bytes and the worker version.
// Keys are content-based, so they survive re-runs and redeploys.
func HashInput(input []byte) string {
	h := blake3.New(32, nil)
	h.Write([]byte(WorkerVersion))
	h.Write(input)
	return hex.EncodeToString(h.Sum(nil))
}

// ReportMaterialization reports the worker's asset materialization with the
// standard cache metadata trio: cache_key, cache_hit, rows.
func ReportMaterialization(
	ctx *dagsterpipes.Context[map[string]any],
	assetKey string,
	dataVersion string,
	cache CacheInfo,
	rows int64,
) error {
	return ctx.ReportAssetMaterialization(&dagsterpipes.AssetMaterialization{
		AssetKey:    assetKey,
		DataVersion: dataVersion,
		Metadata: map[string]any{
			"cache_key":     cache.Key,
			"cache_hit":     cache.Hit,
			"rows":          rows,
			"worker_version": WorkerVersion,
		},
	})
}
