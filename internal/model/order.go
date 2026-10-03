package model

import "time"

type Order struct {
	OrderID   string    `json:"order_id"`
	Status    string    `json:"status"`
	Comment   string    `json:"comment"`
	UpdatedAt time.Time `json:"updated_at"`
}

type UpdateOrderRequest struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
	Comment string `json:"comment"`
}

type UpdateOrderResponse struct {
	OrderID   string    `json:"order_id"`
	UpdatedAt time.Time `json:"updated_at"`
}

type StreamOrderResponse struct {
	Status    string    `json:"status"`
	Comment   string    `json:"comment"`
	Seq       uint64    `json:"seq"`
	Timestamp time.Time `json:"timestamp"`
}
