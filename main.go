package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/celestiaorg/cosmologger/block"
	"github.com/celestiaorg/cosmologger/configs"
	"github.com/celestiaorg/cosmologger/database"
	"github.com/celestiaorg/cosmologger/dbinit"
	"github.com/celestiaorg/cosmologger/tx"
	"github.com/joho/godotenv"

	// "github.com/cosmos/cosmos-sdk/codec"
	sdk "github.com/cosmos/cosmos-sdk/types"
	// "github.com/cosmos/cosmos-sdk/x/auth/legacy/legacytx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	tmClient "github.com/tendermint/tendermint/rpc/client"
	tmClientHttp "github.com/tendermint/tendermint/rpc/client/http"
)

/*--------------*/

const ENV_FILE = "../.env"

func main() {

	if err := godotenv.Load(ENV_FILE); err != nil {
		log.Fatalf("loading environment file `%s`: %v", ENV_FILE, err)
	}

	/*-------------*/

	psqlconn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		os.Getenv("POSTGRES_HOST"),
		os.Getenv("POSTGRES_PORT"),
		os.Getenv("POSTGRES_USER"),
		os.Getenv("POSTGRES_PASSWORD"),
		os.Getenv("POSTGRES_DB"),
	)

	fmt.Printf("\nConnecting to the Database... ")

	db := database.New(database.Postgres, psqlconn)
	defer db.Close()

	// Check if we need to create tables and stuff on the DB
	dbinit.DatabaseInit(db)

	fmt.Printf("Done\n")

	insertQueue := database.NewInsertQueue(db)
	if err := insertQueue.Start(); err != nil {
		fmt.Printf("error in starting insert queue: %v\n", err)
		return
	}
	defer insertQueue.Stop()

	/*-------------*/

	SetBech32Prefixes()

	/*-------------*/

	wsURI := os.Getenv("RPC_ADDRESS")
	// wsURI = "tcp://127.0.0.1:26657"

	fmt.Printf("\nConnecting to the RPC [%s]... ", wsURI)

	//TODO: There is a known issue with the TM client when we use TLS
	// cli, err := tmClient.NewWithClient(wsURI, "/websocket", client)
	cli, err := tmClientHttp.New(wsURI)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Done")

	/*------------------*/

	fmt.Printf("\nStarting the client...\n")

	var cliErr error
	for i := 1; i <= 1000*configs.Configs.TendermintClient.ConnectRetry; i++ {

		fmt.Printf("\r\tTrying to connect #%3.2f", float32(i)/1000.0)
		cliErr = cli.Start()
		if cliErr == nil || errors.Is(cliErr, tmClient.ErrClientRunning) {
			break
		}
		fmt.Printf("\terr: %v", cliErr)
		time.Sleep(100 * time.Microsecond)
	}
	if cliErr != nil && !errors.Is(cliErr, tmClient.ErrClientRunning) {
		panic(cliErr)
	}

	fmt.Println("\nDone")

	/*------------------*/

	// Due to some limitations of the RPC APIs we need to call GRPC ones as well
	grpcCnn, err := GrpcConnect()
	if err != nil {
		log.Fatalf("Did not connect: %s", err)
		return
	}
	defer grpcCnn.Close()

	/*------------------*/

	fmt.Println("\nListening...")
	// Running the listeners
	tx.Start(cli, grpcCnn, db, insertQueue)
	// tx.FixEmptyEvents(cli, grpcCnn, db)
	block.Start(cli, grpcCnn, db, insertQueue)

	/*------------------*/

	// Exit gracefully
	quitChannel := make(chan os.Signal, 1)
	signal.Notify(quitChannel,
		syscall.SIGTERM,
		syscall.SIGINT,
		syscall.SIGQUIT,
		os.Kill, //nolint
		os.Interrupt)
	<-quitChannel

	//Time for cleanup before exit

	if err := cli.UnsubscribeAll(context.Background(), configs.Configs.TendermintClient.SubscriberName); err != nil {
		panic(err)
	}
	if err := cli.Stop(); err != nil {
		panic(err)
	}

	fmt.Println("\nCiao bello!")
}

func GrpcConnect() (*grpc.ClientConn, error) {

	tlsEnabled := os.Getenv("GRPC_TLS")
	GRPCServer := os.Getenv("GRPC_ADDRESS")

	fmt.Printf("\nConnecting to the GRPC [%s] \tTLS: [%s]", GRPCServer, tlsEnabled)

	if strings.ToLower(tlsEnabled) == "true" {
		creds := credentials.NewTLS(&tls.Config{})
		return grpc.Dial(GRPCServer, grpc.WithTransportCredentials(creds))
	}
	return grpc.Dial(GRPCServer, grpc.WithInsecure())

}

func SetBech32Prefixes() {
	config := sdk.GetConfig()
	config.SetBech32PrefixForAccount(configs.Configs.Bech32Prefix.Account.Address, configs.Configs.Bech32Prefix.Account.PubKey)
	config.SetBech32PrefixForValidator(configs.Configs.Bech32Prefix.Validator.Address, configs.Configs.Bech32Prefix.Validator.PubKey)
	config.SetBech32PrefixForConsensusNode(configs.Configs.Bech32Prefix.Consensus.Address, configs.Configs.Bech32Prefix.Consensus.PubKey)
	config.Seal()
}

// // MakeEncodingConfig creates a new EncodingConfig with all modules registered
// func MakeEncodingConfig() params.EncodingConfig {
// 	encodingConfig := params.MakeEncodingConfig()
// 	std.RegisterLegacyAminoCodec(encodingConfig.Amino)
// 	std.RegisterInterfaces(encodingConfig.InterfaceRegistry)
// 	// ModuleBasics.RegisterLegacyAminoCodec(encodingConfig.Amino)
// 	// ModuleBasics.RegisterInterfaces(encodingConfig.InterfaceRegistry)
// 	return encodingConfig
// }
