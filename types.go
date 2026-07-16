package main

type SignOrderRequest struct {
	AccountID uint64 `json:"accountID"`
	SymbolID  uint64 `json:"symbolID"`

	Nonce uint64 `json:"nonce"`

	Side         string `json:"side"`
	PositionSide string `json:"positionSide"`

	Price    string `json:"price"`
	Quantity string `json:"quantity"`

	ClOrdID string `json:"clOrdID"`
}

type SignOrderResponse struct {
	Success bool `json:"success"`

	Signature string `json:"signature,omitempty"`
	Address   string `json:"address,omitempty"`

	Error string `json:"error,omitempty"`
}
