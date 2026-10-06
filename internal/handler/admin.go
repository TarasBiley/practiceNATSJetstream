package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"practiceNATSJetstream/internal/worker"
)

type SyncCacheResponse struct {
	Synced int `json:"synced"`
}

// SyncCache godoc
// @Summary Synchronize Redis cache
// @Description Reads the latest order statuses from JetStream and writes them to Redis
// @Tags admin
// @Produce json
// @Success 200 {object} SyncCacheResponse
// @Failure 500 {string} string "Failed to sync cache"
// @Router /api/admin/sync-cache [post]
func SyncCache(orderConsumer *worker.OrderConsumer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		synced, err := orderConsumer.SyncCache(r.Context())
		if err != nil {
			slog.Error(
				"failed to sync cache",
				"synced", synced,
				"error", err,
			)
			http.Error(
				w,
				"failed to sync cache",
				http.StatusInternalServerError,
			)
			return
		}

		response := SyncCacheResponse{
			Synced: synced,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}
}
