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
		log.Fatal("SODEX_PRIVATE_KEY missing")
	}

	pk, err := crypto.HexToECDSA(key)
	if err != nil {
		log.Fatalf("Invalid private key: %v", err)
	}

	return pk
}

func main() {

	privateKey := loadPrivateKey()

	address := crypto.PubkeyToAddress(privateKey.PublicKey).Hex()

	log.Println("#############################################")
	log.Printf("### SIGNER WALLET: %s", address)
	log.Println("#############################################")


	app := &App{}

	http.HandleFunc("/", app.HealthHandler)
	http.HandleFunc("/sign-order", app.SignOrderHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	server := &http.Server{
		Addr: "0.0.0.0:" + port,
	}

	log.Printf("SERVER LISTENING ON %s", server.Addr)

	err := server.ListenAndServe()
	if err != nil {
		log.Fatalf("SERVER CRASHED: %v", err)
	}
}
