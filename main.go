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

func addTicker(tickerPrices map[string][]domain.Price, price domain.Price) {
	tickerPrices[price.Ticker] = append(tickerPrices[price.Ticker], domain.Price{
		Ticker: price.Ticker,
		Value:  price.Value,
		TS:     price.TS,
	})
}

func fromPricesToMinuteCandles(prices <-chan domain.Price, wg *sync.WaitGroup) <-chan domain.Candle {
	tickerPrices := map[string][]domain.Price{}
	minuteCandlesChan := make(chan domain.Candle, 4)

	wg.Add(1)

	go func() {
		defer wg.Done()
		start, ok := <-prices
		if !ok {
			return
		}

		tickerPrices[start.Ticker] = append(tickerPrices[start.Ticker], domain.Price{
			Ticker: start.Ticker,
			Value:  start.Value,
			TS:     start.TS,
		})

		timeStart, _ := domain.PeriodTS("1m", start.TS)
		timeEnd := timeStart.Add(time.Minute)

		for price := range prices {
			// Если текущий 1-минутный период идет
			if price.TS.Before(timeEnd) {
				tickerPrices[price.Ticker] = append(tickerPrices[price.Ticker], domain.Price{
					Ticker: price.Ticker,
					Value:  price.Value,
					TS:     price.TS,
				})
				// Текущий период окончен: сбор оставшихся трёх тиккеров,
				// обработка накопленных preCandles, создание нового минутного периода
			} else {
				for i := 0; i < 3; i++ {
					tickerPrices[price.Ticker] = append(tickerPrices[price.Ticker], domain.Price{
						Ticker: price.Ticker,
						Value:  price.Value,
						TS:     price.TS,
					})
				}
				minuteCandles := parseMinuteCandles(tickerPrices)
				for _, candle := range minuteCandles {
					minuteCandlesChan <- candle
				}

				// Сброс мапы
				tickerPrices = map[string][]domain.Price{}

				newPeriodStart, _ := domain.PeriodTS("1m", price.TS)
				timeEnd = newPeriodStart.Add(time.Minute - time.Second)

				tickerPrices[price.Ticker] = append(tickerPrices[price.Ticker], domain.Price{
					Ticker: price.Ticker,
					Value:  price.Value,
					TS:     price.TS,
				})
			}
		}
	}()

	return minuteCandlesChan
}

//func fromMinuteTo2MinuteCandles(minuteCandlesChan <-chan domain.Candle, wg *sync.WaitGroup) <-chan domain.Candle {
//	twoMinuteCandlesChan := make(chan domain.Candle)
//	tickerCandles := map[string]domain.Candle{}
//
//	go func() {
//		for candle := range minuteCandlesChan {
//
//		}
//	}()
//
//
//	return twoMinuteCandlesChan
//}

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

	//for i := 0; i <= 20; i++ {
	//	logger.Infof("prices %d: %+v", i, <-prices)
	//}

	wg := sync.WaitGroup{}

	minuteCandlesChan := fromPricesToMinuteCandles(prices, &wg)

	wg.Add(1)
	go func() {
		defer wg.Done()
		counter := 0
		for candle := range minuteCandlesChan {
			if counter%4 == 0 {
				fmt.Println()
			}
			fmt.Println(candle)
			counter++
		}
		//close(minuteCandlesChan)
	}()

	wg.Wait()
}
