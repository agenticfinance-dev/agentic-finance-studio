package main

import (
	"context"
	"crypto/ecdsa"
	"fmt"

	"github.com/sodex-tech/sodex-go-sdk-public/client"
)

type SoDEXClient struct {
	Client *client.Client
}

func NewSoDEXClient(privateKey *ecdsa.PrivateKey) *SoDEXClient {
	cfg := client.Config{
		BaseURL:    "https://api.sodex.xyz",
		ChainID:    286623,
		PrivateKey: privateKey,
	}

	return &SoDEXClient{
		Client: client.New(cfg),
	}
}

func (s *SoDEXClient) Health() string {
	return "SoDEX SDK Connected"
}

func (s *SoDEXClient) GetSymbols(ctx context.Context) ([]client.Symbol, error) {
	symbols, err := s.Client.PerpsSymbols(ctx)
	if err != nil {
		return nil, err
	}
	return symbols, nil
}

func (s *SoDEXClient) Address() string {
	return fmt.Sprintf("%s", s.Client.Address())
}
