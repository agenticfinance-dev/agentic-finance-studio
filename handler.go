package main

import (
	"context"
	"encoding/json"
	"net/http"
)

type App struct {
	SoDEX *SoDEXClient
}

func (a *App) HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	json.NewEncoder(w).Encode(map[string]any{
		"status":  "ok",
		"service": "SoDEX Go SDK",
	})
}

func (a *App) SymbolsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	symbols, err := a.SoDEX.GetSymbols(context.Background())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(symbols)
}

// NEW HANDLER
func (a *App) SignOrderHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	var req SignOrderRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(SignOrderResponse{
			Success: false,
			Error: err.Error(),
		})
		return
	}

	signer, err := NewOrderSigner(req.PrivateKey)
	if err != nil {
		json.NewEncoder(w).Encode(SignOrderResponse{
			Success: false,
			Error: err.Error(),
		})
		return
	}

	json.NewEncoder(w).Encode(SignOrderResponse{
		Success: true,
		Address: signer.Address(),
	})
}
