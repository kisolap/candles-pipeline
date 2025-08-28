package main

import (
	"context"
	"hw1/hw3/domain"
	"hw1/hw3/generator"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

var tickers = []string{"AAPL", "SBER", "NVDA", "TSLA"}

func main() {
	logger := log.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	file1, _ := os.Create("candles_1m.csv")
	file2, _ := os.Create("candles_2m.csv")
	file3, _ := os.Create("candles_10m.csv")
	defer file1.Close()
	defer file2.Close()
	defer file3.Close()

	writer := domain.CandleWriter{
		File1: file1,
		File2: file2,
		File3: file3,
	}

	pg := generator.NewPricesGenerator(generator.Config{
		Factor:  10,
		Delay:   time.Millisecond * 500,
		Tickers: tickers,
	})

	logger.Info("start prices generator...")
	prices := pg.Prices(ctx)

	wg := sync.WaitGroup{}

	minuteCandlesChan := domain.FromPricesToMinuteCandles(prices, &wg, &writer)
	twoMinuteCandlesChan := domain.CreateCandles(minuteCandlesChan, &wg, &writer, domain.CandlePeriod2m)
	domain.CreateCandles(twoMinuteCandlesChan, &wg, &writer, domain.CandlePeriod10m)

	wg.Wait()
}
