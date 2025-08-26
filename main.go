package main

import (
	"context"
	"fmt"
	"hw1/hw3/domain"
	"hw1/hw3/generator"
	"math"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

var tickers = []string{"AAPL", "SBER", "NVDA", "TSLA"}

// Переделать на возврат channel
func parseMinuteCandles(tickerPrices map[string][]domain.Price) []domain.Candle {
	var minuteCandles []domain.Candle

	for _, ticker := range tickerPrices {
		name := ticker[0].Ticker
		period := domain.CandlePeriod1m
		low := float64(math.MaxInt64)
		high := float64(math.MinInt64)
		opened := ticker[0].Value
		closed := ticker[len(ticker)-1].Value
		ts := ticker[0].TS

		for _, price := range ticker {
			if price.Value > high {
				high = price.Value
			}
			if price.Value < low {
				low = price.Value
			}
		}

		candle := domain.Candle{
			Ticker: name,
			Period: period,
			Open:   opened,
			High:   high,
			Low:    low,
			Close:  closed,
			TS:     ts,
		}

		minuteCandles = append(minuteCandles, candle)
	}

	return minuteCandles
}

func fromPricesToMinuteCandles(prices <-chan domain.Price, wg *sync.WaitGroup) <-chan domain.Candle {
	tickerPrices := map[string][]domain.Price{}
	minuteCandles := make(chan domain.Candle)

	wg.Add(1)

	go func() {
		defer wg.Done()
		start, ok := <-prices
		if !ok {
			return
		}

		timeStart, _ := domain.PeriodTS("1m", start.TS)
		timeEnd := timeStart.Add(time.Minute)

		for price := range prices {
			// Если текущий 1-минутный период идет
			if price.TS.Before(timeEnd) {
				tickerPrices[price.Ticker] = append(tickerPrices[price.Ticker], domain.Price{
					Value: price.Value,
					TS:    price.TS,
				})
				// Текущий период окончен: обработка накопленных preCandles,
				// создание нового минутного периода
			} else {
				fmt.Println(parseMinuteCandles(tickerPrices))

				tickerPrices = map[string][]domain.Price{}

				newPeriodStart, _ := domain.PeriodTS("1m", price.TS)
				timeEnd = newPeriodStart.Add(time.Minute - time.Second)

				tickerPrices[price.Ticker] = append(tickerPrices[price.Ticker], domain.Price{
					Value: price.Value,
					TS:    price.TS,
				})
			}
		}
	}()

	return minuteCandles
}

func main() {
	logger := log.New()
	ctx, cancel := context.WithCancel(context.Background())

	pg := generator.NewPricesGenerator(generator.Config{
		Factor:  10,
		Delay:   time.Millisecond * 500,
		Tickers: tickers,
	})

	logger.Info("start prices generator...")
	prices := pg.Prices(ctx)

	for i := 0; i <= 10; i++ {
		logger.Infof("prices %d: %+v", i, <-prices)
	}

	wg := sync.WaitGroup{}

	fromPricesToMinuteCandles(prices, &wg)

	wg.Wait()
	cancel()
}
