package main

type SignOrderRequest struct {
	PrivateKey string `json:"private_key"`
	AccountID  uint64 `json:"account_id"`
	SymbolID   uint64 `json:"symbol_id"`

	Side         string `json:"side"`
	PositionSide string `json:"position_side"`

	Price    string `json:"price"`
	Quantity string `json:"quantity"`

	ClOrdID string `json:"cl_ord_id"`
}

type SignOrderResponse struct {
	Success   bool   `json:"success"`
	Signature string `json:"signature,omitempty"`
	Address   string `json:"address,omitempty"`
	Error     string `json:"error,omitempty"`
}
