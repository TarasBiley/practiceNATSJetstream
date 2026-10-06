package handler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"practiceNATSJetstream/internal/model"
	natsclient "practiceNATSJetstream/internal/nats"
	"practiceNATSJetstream/internal/repository"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go/jetstream"
)

type orderUpdater interface {
	ClaimIdempotency(context.Context, string, string, string) (repository.IdempotencyClaim, error)
}

type orderPublisher interface {
	GetLastOrderStatus(context.Context, string) (*jetstream.RawStreamMsg, error)
	PublishOrderStatus(context.Context, string, []byte, string, uint64) (*jetstream.PubAck, error)
	PrintStreamInfo(context.Context) error
}

// UpdateOrder godoc
// @Summary Update order status
// @Description Publish the order status to JetStream, then save it to PostgreSQL. Retrying the same key and body resumes an unfinished request or returns its original result.
// @Tags orders
// @Accept json
// @Produce json
// @Param Idempotency-Key header string true "Unique request key; reuse only with the same body"
// @Param request body model.UpdateOrderRequest true "Order status"
// @Success 200 {object} model.UpdateOrderResponse
// @Failure 400 {string} string "Bad request"
// @Failure 409 {string} string "Key reused with a different body, request in progress, or concurrent order update"
// @Failure 500 {string} string "Internal server error; retry with the same key and body"
// @Router /api/orders [post]
func UpdateOrder(repo orderUpdater, natsClient orderPublisher) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Idempotency-Key")
		if strings.TrimSpace(key) == "" || len(key) > 256 {
			http.Error(w, "Idempotency-Key header is required (maximum 256 bytes)", http.StatusBadRequest)
			return
		}
		var req model.UpdateOrderRequest
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			http.Error(w, "request body must contain one JSON object", http.StatusBadRequest)
			return
		}
		if !validOrderID(req.OrderID) {
			http.Error(w, "invalid order_id", http.StatusBadRequest)
			return
		}
		if !validStatus(req.Status) {
			http.Error(w, "invalid status", http.StatusBadRequest)
			return
		}
		requestHash, err := makeRequestHash(req)
		if err != nil {
			slog.Error("failed to hash request", "order_id", req.OrderID, "error", err)
			http.Error(w, "failed to process request", http.StatusInternalServerError)
			return
		}
		// Bound DB acquisition, publication and completion even without a client deadline.
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		claim, err := repo.ClaimIdempotency(ctx, key, requestHash, req.OrderID)
		if err != nil {
			if errors.Is(err, repository.ErrIdempotencyConflict) || errors.Is(err, repository.ErrRequestInProgress) {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
			slog.Error("failed to claim idempotency key", "order_id", req.OrderID, "error", err)
			http.Error(w, "failed to process idempotency key", http.StatusInternalServerError)
			return
		}
		defer func() {
			if err := claim.Close(); err != nil {
				slog.Error("failed to release idempotency attempt", "order_id", req.OrderID, "error", err)
			}
		}()
		record := claim.Record()
		if record.Completed {
			if record.UpdatedAt == nil {
				slog.Error("completed idempotency record has no timestamp", "order_id", req.OrderID)
				http.Error(w, "invalid idempotency record", http.StatusInternalServerError)
				return
			}
			writeOrderResult(w, record.OrderID, *record.UpdatedAt)
			return
		}
		updatedAt := time.Now().UTC().Truncate(time.Microsecond)
		if record.UpdatedAt != nil {
			updatedAt = record.UpdatedAt.UTC()
		}
		order := model.Order{OrderID: req.OrderID, Status: req.Status, Comment: req.Comment, UpdatedAt: updatedAt}
		// Marshal before persisting publication intent. Close frees an unprepared claim
		// even when the request context was canceled.
		data, err := json.Marshal(order)
		if err != nil {
			slog.Error("failed to encode order event", "order_id", req.OrderID, "error", err)
			http.Error(w, "failed to encode event", http.StatusInternalServerError)
			return
		}
		if record.ExpectedSequence == nil {
			var expectedSequence uint64
			last, err := natsClient.GetLastOrderStatus(ctx, req.OrderID)
			if err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
				slog.Error("failed to prepare publication", "order_id", req.OrderID, "error", err)
				http.Error(w, "failed to prepare publication", http.StatusInternalServerError)
				return
			}
			if err == nil {
				expectedSequence = last.Sequence
			}
			if err := claim.Prepare(ctx, updatedAt, expectedSequence); err != nil {
				slog.Error("failed to persist publication intent", "order_id", req.OrderID, "error", err)
				http.Error(w, "failed to prepare publication", http.StatusInternalServerError)
				return
			}
			record = claim.Record()
		}
		if record.PublishedSequence == nil {
			// Hash the opaque HTTP key to keep the NATS header deterministic and safe.
			msgHash := sha256.Sum256([]byte(key))
			ack, err := natsClient.PublishOrderStatus(ctx, req.OrderID, data, hex.EncodeToString(msgHash[:]), *record.ExpectedSequence)
			if err != nil {
				slog.Error("failed to publish order", "order_id", req.OrderID, "error", err)
				// Retain the prepared record on every publish error. A timeout does not prove
				// absence of a side effect; the same key can retry safely using the saved CAS.
				if errors.Is(err, natsclient.ErrPublishConflict) {
					http.Error(w, "concurrent order update; original publication requires reconciliation", http.StatusConflict)
				} else {
					http.Error(w, "failed to publish event; retry with the same key", http.StatusInternalServerError)
				}
				return
			}
			slog.Info("order status published", "order_id", req.OrderID, "stream", ack.Stream,
				"subject", "order.status."+req.OrderID, "seq", ack.Sequence, "duplicate", ack.Duplicate)
			if err := claim.MarkPublished(ctx, ack.Sequence); err != nil {
				slog.Error("failed to record publication", "order_id", req.OrderID, "error", err)
				http.Error(w, "failed to record publication; retry with the same key", http.StatusInternalServerError)
				return
			}
		}
		// The order UPSERT and idempotency completion share one PostgreSQL transaction.
		if err := claim.Complete(ctx, order); err != nil {
			slog.Error("failed to save order and complete request", "order_id", req.OrderID, "error", err)
			http.Error(w, "failed to save order; retry with the same key", http.StatusInternalServerError)
			return
		}
		if err := natsClient.PrintStreamInfo(ctx); err != nil {
			slog.Error("failed to get stream info", "order_id", req.OrderID, "error", err)
		}
		writeOrderResult(w, order.OrderID, order.UpdatedAt)
	}
}

func writeOrderResult(w http.ResponseWriter, orderID string, updatedAt time.Time) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(model.UpdateOrderResponse{OrderID: orderID, UpdatedAt: updatedAt.UTC()}); err != nil {
		slog.Warn("failed to write order response", "order_id", orderID, "error", err)
	}
}

func validStatus(status string) bool {
	switch status {
	case "created", "paid", "shipped", "delivered":
		return true
	default:
		return false
	}
}

func makeRequestHash(
	req model.UpdateOrderRequest,
) (string, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return "", err
	}

	hash := sha256.Sum256(data)

	return hex.EncodeToString(hash[:]), nil
}

// GetOrder godoc
// @Summary Get current order status
// @Description Returns the current order status from PostgreSQL
// @Tags orders
// @Produce json
// @Param order_id path string true "Order ID"
// @Success 200 {object} model.Order
// @Failure 400 {string} string "Invalid order_id"
// @Failure 404 {string} string "Order not found"
// @Failure 500 {string} string "Internal server error"
// @Router /api/orders/{order_id} [get]
func GetOrder(repo *repository.OrderRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		orderID := r.PathValue("order_id")

		if !validOrderID(orderID) {
			http.Error(w, "invalid order_id", http.StatusBadRequest)
			return
		}

		order, err := repo.GetByID(r.Context(), orderID)
		if err != nil {
			if err == pgx.ErrNoRows {
				http.Error(w, "order not found", http.StatusNotFound)
				return
			}

			slog.Error(
				"failed to get order",
				"order_id", orderID,
				"error", err,
			)

			http.Error(w, "failed to get order", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		json.NewEncoder(w).Encode(order)
	}
}

// GetOrderFromStream godoc
// @Summary Get current order status from JetStream
// @Description Uses JetStream Direct Get to obtain the latest message without creating a consumer
// @Tags orders
// @Produce json
// @Param order_id path string true "Order ID"
// @Success 200 {object} model.StreamOrderResponse
// @Failure 400 {string} string "Invalid order_id"
// @Failure 404 {string} string "Order status not found in stream"
// @Failure 504 {string} string "JetStream request timeout"
// @Failure 500 {string} string "Internal server error"
// @Router /api/stream/orders/{order_id} [get]
func GetOrderFromStream(
	natsClient *natsclient.Client,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		orderID := r.PathValue("order_id")

		if !validOrderID(orderID) {
			http.Error(w, "invalid order_id", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(
			r.Context(),
			2*time.Second,
		)
		defer cancel()

		msg, err := natsClient.GetLastOrderStatus(ctx, orderID)
		if err != nil {
			if errors.Is(err, jetstream.ErrMsgNotFound) {
				http.Error(
					w,
					"order status not found in stream",
					http.StatusNotFound,
				)
				return
			}

			if errors.Is(err, context.DeadlineExceeded) {
				slog.Error(
					"timed out getting order from stream",
					"order_id", orderID,
					"error", err,
				)

				http.Error(
					w,
					"JetStream request timeout",
					http.StatusGatewayTimeout,
				)
				return
			}

			slog.Error(
				"failed to get order from stream",
				"order_id", orderID,
				"error", err,
			)

			http.Error(
				w,
				"failed to get order status from stream",
				http.StatusInternalServerError,
			)
			return
		}

		var order model.Order

		err = json.Unmarshal(msg.Data, &order)
		if err != nil {
			slog.Error(
				"failed to decode stream message",
				"order_id", orderID,
				"error", err,
			)

			http.Error(
				w,
				"failed to decode stream message",
				http.StatusInternalServerError,
			)
			return
		}

		slog.Info(
			"order status read from stream",
			"stream", "ORDERS",
			"subject", msg.Subject,
			"seq", msg.Sequence,
		)

		response := model.StreamOrderResponse{
			Status:    order.Status,
			Comment:   order.Comment,
			Seq:       msg.Sequence,
			Timestamp: msg.Time,
		}

		w.Header().Set("Content-Type", "application/json")

		json.NewEncoder(w).Encode(response)
	}
}
