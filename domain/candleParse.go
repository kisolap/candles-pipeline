package domain

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"sync"
	"time"
)

type CandleWriter struct {
	File1 io.Writer
	File2 io.Writer
	File3 io.Writer
}

func (w *CandleWriter) saveCandle(period CandlePeriod, candle Candle) {
	var file *csv.Writer
	switch period {
	case CandlePeriod1m:
		file = csv.NewWriter(w.File1)
	case CandlePeriod2m:
		file = csv.NewWriter(w.File2)
	case CandlePeriod10m:
		file = csv.NewWriter(w.File3)
	}

	file.Write([]string{candle.Ticker, candle.TS.String(),
		fmt.Sprintf("%f", candle.Open),
		fmt.Sprintf("%f", candle.High),
		fmt.Sprintf("%f", candle.Low),
		fmt.Sprintf("%f", candle.Close),
	})

	file.Flush()
}

// Безопасное добавление в мапу (при увеличение слайса мог измениться адрес)
func addTicker(tickerPrices map[string][]Price, price Price) {
	tickerSlice := tickerPrices[price.Ticker]
	tickerSlice = append(tickerPrices[price.Ticker], Price{
		Ticker: price.Ticker,
		Value:  price.Value,
		TS:     price.TS,
	})
	tickerPrices[price.Ticker] = tickerSlice
}

// Переделать на возврат channel
func parseMinuteCandles(tickerPrices map[string][]Price) []Candle {
	var minuteCandles []Candle

	for _, ticker := range tickerPrices {
		name := ticker[0].Ticker
		period := CandlePeriod1m
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

		candle := Candle{
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

func parseCandles(candlePeriod CandlePeriod, tickerCandles map[string][]Candle) []Candle {
	var parsedCandles []Candle

	for _, candles := range tickerCandles {
		ticker := candles[0].Ticker
		period := candlePeriod
		opened := candles[0].Open
		low := float64(math.MaxInt64)
		high := float64(math.MinInt64)
		closed := candles[len(candles)-1].Close
		ts := candles[0].TS

		for _, candle := range candles {
			if candle.High > high {
				high = candle.High
			}
			if candle.Low < low {
				low = candle.Low
			}
		}

		candle := Candle{
			Ticker: ticker,
			Period: period,
			Open:   opened,
			High:   high,
			Low:    low,
			Close:  closed,
			TS:     ts,
		}

		parsedCandles = append(parsedCandles, candle)
	}

	return parsedCandles
}

func FromPricesToMinuteCandles(prices <-chan Price, wg *sync.WaitGroup, writer *CandleWriter) chan Candle {
	tickerPrices := map[string][]Price{}
	minuteCandlesChan := make(chan Candle)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(minuteCandlesChan)
		start, ok := <-prices
		if !ok {
			return
		}

		addTicker(tickerPrices, start)

		timeStart, _ := PeriodTS("1m", start.TS)
		timeEnd := timeStart.Add(time.Minute)

		for price := range prices {
			//fmt.Println("prices: %+v", price)
			if price.TS.Before(timeEnd) {
				addTicker(tickerPrices, price)
			} else {
				minuteCandles := parseMinuteCandles(tickerPrices)
				for _, candle := range minuteCandles {
					writer.saveCandle(CandlePeriod1m, candle)
					minuteCandlesChan <- candle
				}

				// Сброс мапы
				tickerPrices = map[string][]Price{}

				newPeriodStart, _ := PeriodTS("1m", price.TS)
				timeEnd = newPeriodStart.Add(time.Minute)

				addTicker(tickerPrices, price)
			}
		}
	}()

	return minuteCandlesChan
}

func CreateCandles(inCandlesChan <-chan Candle, wg *sync.WaitGroup, writer *CandleWriter, period CandlePeriod) chan Candle {
	outCandlesChan := make(chan Candle)
	tickerCandles := map[string][]Candle{}

	wg.Add(1)
	go func() {
		defer wg.Done()

		start, ok := <-inCandlesChan
		if !ok {
			return
		}

		timeStart, _ := PeriodTS(period, start.TS)

		var minuteCount int64
		switch period {
		case CandlePeriod1m:
			minuteCount = 1
		case CandlePeriod2m:
			minuteCount = 2
		case CandlePeriod10m:
			minuteCount = 10
		}

		timeEnd := timeStart.Add(time.Minute * time.Duration(minuteCount))
		tickerCandles[start.Ticker] = append(tickerCandles[start.Ticker], start)

		for candle := range inCandlesChan {
			if candle.TS.Before(timeEnd) {
				tickerCandles[candle.Ticker] = append(tickerCandles[candle.Ticker], candle)
			} else {
				candles := parseCandles(period, tickerCandles)
				for _, newCandle := range candles {
					if period == CandlePeriod2m {
						outCandlesChan <- newCandle
					}
					writer.saveCandle(period, newCandle)
				}

				tickerCandles = map[string][]Candle{}

				newPeriodStart, _ := PeriodTS(period, candle.TS)
				timeEnd = newPeriodStart.Add(time.Minute * time.Duration(minuteCount))

				tickerCandles[candle.Ticker] = append(tickerCandles[candle.Ticker], candle)
			}
		}

		close(outCandlesChan)
	}()

	return outCandlesChan
}
