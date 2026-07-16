package main

import (
	"crypto/ecdsa"
	"log"
	"net/http"
	"os"

	"github.com/ethereum/go-ethereum/crypto"
)

func loadPrivateKey() *ecdsa.PrivateKey {
	key := os.Getenv("SODEX_PRIVATE_KEY")
	if key == "" {
		log.Fatal("SODEX_PRIVATE_KEY environment variable not set")
	}

	pk, err := crypto.HexToECDSA(key)
	if err != nil {
		log.Fatalf("Invalid private key: %v", err)
	}

	return pk
}

func main() {
	privateKey := loadPrivateKey()

	sodex := NewSoDEXClient(privateKey)

	app := &App{
		SoDEX: sodex,
	}

	http.HandleFunc("/", app.HealthHandler)
	http.HandleFunc("/symbols", app.SymbolsHandler)
	http.HandleFunc("/sign-order", app.SignOrderHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Server running on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
