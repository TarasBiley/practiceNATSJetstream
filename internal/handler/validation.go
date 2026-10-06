package handler

import "regexp"

var orderIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

func validOrderID(orderID string) bool {
	return orderIDPattern.MatchString(orderID)
}
