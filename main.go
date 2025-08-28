package main

import (
	"context"
	"fmt"
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

	pg := generator.NewPricesGenerator(generator.Config{
		Factor:  10,
		Delay:   time.Millisecond * 500,
		Tickers: tickers,
	})

	logger.Info("start prices generator...")
	prices := pg.Prices(ctx)

	file1, _ := os.Create("candles_1m.csv")
	file2, _ := os.Create("candles_2m.csv")
	file3, _ := os.Create("candles_10m.csv")
	defer file1.Close()
	defer file2.Close()
	defer file3.Close()

	wg := sync.WaitGroup{}
	writer := domain.CandleWriter{
		File1: file1,
		File2: file2,
		File3: file3,
	}

	minuteCandlesChan := domain.FromPricesToMinuteCandles(prices, &wg, &writer)
	twoMinuteCandlesChan := domain.FromMinuteTo2MinuteCandles(minuteCandlesChan, &wg, &writer)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for candle := range twoMinuteCandlesChan {
			fmt.Println(candle)
		}
	}()

	//wg.Add(1)
	//go func() {
	//	defer wg.Done()
	//	counter := 0
	//	for candle := range minuteCandlesChan {
	//		if counter%4 == 0 {
	//			fmt.Println()
	//		}
	//		fmt.Println(candle)
	//		counter++
	//	}
	//}()

	wg.Wait()
}
