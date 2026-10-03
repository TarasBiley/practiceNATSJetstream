package handler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"practiceNATSJetstream/internal/model"
	natsclient "practiceNATSJetstream/internal/nats"
	"practiceNATSJetstream/internal/repository"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go/jetstream"
)

// UpdateOrder godoc
// @Summary Update order status
// @Description Save current order status to PostgreSQL and publish event to NATS JetStream
// @Tags orders
// @Accept json
// @Produce json
// @Param request body model.UpdateOrderRequest true "Order status"
// @Success 200 {object} model.UpdateOrderResponse
// @Failure 400 {string} string "Bad request"
// @Failure 500 {string} string "Internal server error"
// @Router /api/orders [post]
func UpdateOrder(
	repo *repository.OrderRepository,
	natsClient *natsclient.Client,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req model.UpdateOrderRequest

		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}

		if req.OrderID == "" {
			http.Error(w, "order_id is required", http.StatusBadRequest)
			return
		}

		if !validStatus(req.Status) {
			http.Error(w, "invalid status", http.StatusBadRequest)
			return
		}

		order := model.Order{
			OrderID: req.OrderID,
			Status:  req.Status,
			Comment: req.Comment,
		}

		updatedAt, err := repo.Save(r.Context(), order)
		if err != nil {
			http.Error(w, "failed to save order", http.StatusInternalServerError)
			return
		}
		event := model.Order{
			OrderID:   req.OrderID,
			Status:    req.Status,
			Comment:   req.Comment,
			UpdatedAt: updatedAt,
		}

		data, err := json.Marshal(event)
		if err != nil {
			http.Error(w, "failed to encode event", http.StatusInternalServerError)
			return
		}

		msgID := makeMsgID(req.OrderID, updatedAt)

		ack, err := natsClient.PublishOrderStatus(
			r.Context(),
			req.OrderID,
			data,
			msgID,
		)

		log.Printf(
			"stream=%s subject=%s seq=%d",
			ack.Stream,
			"order.status."+req.OrderID,
			ack.Sequence,
		)
		if err != nil {
			http.Error(w, "failed to publish event", http.StatusInternalServerError)
			return
		}

		err = natsClient.PrintStreamInfo(r.Context())
		if err != nil {
			log.Printf("failed to get stream info: %v", err)
		}
		response := model.UpdateOrderResponse{
			OrderID:   req.OrderID,
			UpdatedAt: updatedAt,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		json.NewEncoder(w).Encode(response)
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

func makeMsgID(orderID string, updatedAt time.Time) string {
	data := orderID + updatedAt.String()

	hash := sha256.Sum256([]byte(data))

	return hex.EncodeToString(hash[:])
}

// GetOrder godoc
// @Summary Get current order status
// @Description Returns the current order status from PostgreSQL
// @Tags orders
// @Produce json
// @Param order_id path string true "Order ID"
// @Success 200 {object} model.Order
// @Failure 404 {string} string "Order not found"
// @Failure 500 {string} string "Internal server error"
// @Router /api/orders/{order_id} [get]
func GetOrder(repo *repository.OrderRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		orderID := r.PathValue("order_id")

		order, err := repo.GetByID(r.Context(), orderID)
		if err != nil {

			if err == pgx.ErrNoRows {
				http.Error(w, "order not found", http.StatusNotFound)
				return
			}

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
// @Failure 404 {string} string "Order status not found in stream"
// @Failure 504 {string} string "JetStream request timeout"
// @Failure 500 {string} string "Internal server error"
// @Router /api/stream/orders/{order_id} [get]
func GetOrderFromStream(
	natsClient *natsclient.Client,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		orderID := r.PathValue("order_id")

		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
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
				http.Error(
					w,
					"JetStream request timeout",
					http.StatusGatewayTimeout,
				)
				return
			}

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
			http.Error(
				w,
				"failed to decode stream message",
				http.StatusInternalServerError,
			)
			return
		}

		log.Printf(
			"stream=ORDERS subject=%s seq=%d",
			msg.Subject,
			msg.Sequence,
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
