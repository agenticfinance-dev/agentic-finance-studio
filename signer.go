package main

import (
	"crypto/ecdsa"
	"encoding/hex"

	"github.com/ethereum/go-ethereum/crypto"

	perpsSigner "github.com/sodex-tech/sodex-go-sdk-public/perps/signer"
)

const chainID uint64 = 286623

type OrderSigner struct {
	privateKey *ecdsa.PrivateKey
	signer     *perpsSigner.Signer
}

func NewOrderSigner(privateKeyHex string) (*OrderSigner, error) {
	privateKey, err := crypto.HexToECDSA(privateKeyHex)
	if err != nil {
		return nil, err
	}

	return &OrderSigner{
		privateKey: privateKey,
		signer:     perpsSigner.NewSigner(chainID, privateKey),
	}, nil
}

func (o *OrderSigner) Address() string {
	return crypto.PubkeyToAddress(o.privateKey.PublicKey).Hex()
}

func EncodeSignature(sig []byte) string {
	return hex.EncodeToString(sig)
}
