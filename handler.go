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
