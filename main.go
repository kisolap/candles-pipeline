package main

import (
	"context"
	"hw1/domain"
	"hw1/generator"
	"os"
	"os/signal"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

var tickers = []string{"AAPL", "SBER", "NVDA", "TSLA"}

func main() {
	logger := log.New()
	ctx, cancel := context.WithCancel(context.Background())

	file1, _ := os.Create("candles_1m.csv")
	file2, _ := os.Create("candles_2m.csv")
	file3, _ := os.Create("candles_10m.csv")
	defer file1.Close()
	defer file2.Close()
	defer file3.Close()

	writer := domain.NewCandleWriter(file1, file2, file3)

	pg := generator.NewPricesGenerator(generator.Config{
		Factor:  10,
		Delay:   time.Millisecond * 500,
		Tickers: tickers,
	})

	logger.Info("Start prices generator...")

	prices := pg.Prices(ctx)

	wg := sync.WaitGroup{}
	domain.ProcessPrices(prices, &wg, writer)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	sig := <-sigChan
	logger.Infof("Received %v, start graceful shutdown...", sig)
	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("All goroutines finished successfully.")
	case <-time.After(10 * time.Second):
		logger.Info("Graceful shutdown timeout, some goroutines may not have finished.")
	}

	logger.Info("Program shutdown complete.")
}
