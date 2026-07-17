package main

import (
    "context"
    "encoding/json"
    "log"
    "net/http"
)

type App struct {
    SoDEX *SoDEXClient
}

func (a *App) HealthHandler(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]any{
        "status": "ok",
        "service": "SoDEX Go SDK",
    })
}

func (a *App) SymbolsHandler(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    symbols, err := a.SoDEX.GetSymbols(context.Background())
    if err!= nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    json.NewEncoder(w).Encode(symbols)
}

// SignOrderHandler signs a new perps order.
func (a *App) SignOrderHandler(w http.ResponseWriter, r *http.Request) {
    if r.Method!= http.MethodPost {
        http.Error(w, "POST required", http.StatusMethodNotAllowed)
        return
    }

    w.Header().Set("Content-Type", "application/json")

    var req SignOrderRequest

    if err := json.NewDecoder(r.Body).Decode(&req); err!= nil {
        json.NewEncoder(w).Encode(SignOrderResponse{
            Success: false,
            Error: err.Error(),
        })
        return
    }

    // Use the server private key from Render
    privateKey := loadPrivateKey()

    signer, err := NewOrderSigner(privateKey)
    if err!= nil {
        json.NewEncoder(w).Encode(SignOrderResponse{
            Success: false,
            Error: err.Error(),
        })
        return
    }

    signature, err := signer.Sign(req)
    if err!= nil {
        json.NewEncoder(w).Encode(SignOrderResponse{
            Success: false,
            Error: err.Error(),
        })
        return
    }

    log.Printf("========== SIGN REQUEST ==========")
    log.Printf("AccountID: %d", req.AccountID)
    log.Printf("SymbolID : %d", req.SymbolID)
    log.Printf("Nonce : %d", req.Nonce)
    log.Printf("Side : %s", req.Side)
    log.Printf("PosSide : %s", req.PositionSide)
    log.Printf("Price : %s", req.Price)
    log.Printf("Quantity : %s", req.Quantity)
    log.Printf("ClOrdID : %s", req.ClOrdID)
    log.Printf("Signature: %s", signature)
    log.Printf("==================================")

    json.NewEncoder(w).Encode(SignOrderResponse{
        Success: true,
        Signature: signature,
        Address: signer.Address(),
    })
}
