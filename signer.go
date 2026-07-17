package main

import (
	"encoding/hex"

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

func NewOrderSigner(privateKeyHex string) (*OrderSigner, error) {

	privateKey, err := crypto.HexToECDSA(privateKeyHex)
	if err != nil {
		return nil, err
	}

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

	price, err := decimal.NewFromString(req.Price)
	if err != nil {
		return "", err
	}

	qty, err := decimal.NewFromString(req.Quantity)
	if err != nil {
		return "", err
	}

	// Convert order side
	var side enums.OrderSide
	switch req.Side {
	case "BUY":
		side = enums.OrderSideBuy
	case "SELL":
		side = enums.OrderSideSell
	default:
		side = enums.OrderSideUnknown
	}

	// Convert position side
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
