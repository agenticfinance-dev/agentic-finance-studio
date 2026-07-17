package main

import (
    "crypto/ecdsa"
    "encoding/hex"
    "log"

    "github.com/ethereum/go-ethereum/crypto"
    "github.com/shopspring/decimal"

    "github.com/sodex-tech/sodex-go-sdk-public/common/enums"
    perpsSigner "github.com/sodex-tech/sodex-go-sdk-public/perps/signer"
    perpsTypes "github.com/sodex-tech/sodex-go-sdk-public/perps/types"
)

type OrderSigner struct {
    signer  *perpsSigner.Signer
    address string
}

func NewOrderSigner(privateKey *ecdsa.PrivateKey) (*OrderSigner, error) {
    s := perpsSigner.NewSigner(286623, privateKey)
    address := crypto.PubkeyToAddress(privateKey.PublicKey).Hex()
    return &OrderSigner{
        signer:  s,
        address: address,
    }, nil
}

func (o *OrderSigner) Address() string {
    return o.address
}

func (o *OrderSigner) Sign(req SignOrderRequest) (string, error) {
    log.Println("========== GO SIGNER ==========")
    log.Printf("AccountID: %d", req.AccountID)
    log.Printf("SymbolID : %d", req.SymbolID)
    log.Printf("Nonce    : %d", req.Nonce)
    log.Printf("Price    : %s", req.Price)
    log.Printf("Quantity : %s", req.Quantity)
    log.Printf("Side     : %s", req.Side)
    log.Printf("PosSide  : %s", req.PositionSide)
    log.Println("===============================")

    price, err := decimal.NewFromString(req.Price)
    if err != nil {
        return "", err
    }

    qty, err := decimal.NewFromString(req.Quantity)
    if err != nil {
        return "", err
    }

    var side enums.OrderSide
    switch req.Side {
    case "BUY":
        side = enums.OrderSideBuy
    case "SELL":
        side = enums.OrderSideSell
    default:
        side = enums.OrderSideUnknown
    }

    var positionSide enums.PositionSide
    switch req.PositionSide {
    case "LONG":
        positionSide = enums.PositionSideLong
    case "SHORT":
        positionSide = enums.PositionSideShort
    case "BOTH":
        positionSide = enums.PositionSideBoth
    default:
        positionSide = enums.PositionSideUnknown
    }

    order := &perpsTypes.NewOrderRequest{
        AccountID: req.AccountID,
        SymbolID:  req.SymbolID,
        Orders: []*perpsTypes.RawOrder{
            {
                ClOrdID:      req.ClOrdID,
                Modifier:     enums.OrderModifierNormal,
                Side:         side,
                Type:         enums.OrderTypeLimit,
                TimeInForce:  enums.TimeInForceGTC,
                Price:        &price,
                Quantity:     &qty,
                PositionSide: positionSide,
            },
        },
    }

    sig, err := o.signer.SignNewOrderRequest(order, req.Nonce)
    if err != nil {
        return "", err
    }

    return hex.EncodeToString(sig), nil
}
