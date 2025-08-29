package domain

import (
	"context"
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

func ProcessPrices(ctx context.Context, prices <-chan Price, wg *sync.WaitGroup, writer *CandleWriter) {
	wg.Add(1)
	outCandles := make(chan Candle)
	go func() {
		defer wg.Done()
		defer close(outCandles)

		for {
			select {
			case price, ok := <-prices:
				if !ok {
					return
				}
				candle := Candle{
					Ticker: price.Ticker,
					Period: CandlePeriod1m,
					Open:   price.Value,
					High:   price.Value,
					Low:    price.Value,
					Close:  price.Value,
					TS:     price.TS,
				}
				outCandles <- candle
			case <-ctx.Done():
				return
			}
		}
	}()

	minuteCandlesChan := CreateCandles(ctx, outCandles, wg, writer, CandlePeriod1m)
	twoMinuteCandlesChan := CreateCandles(ctx, minuteCandlesChan, wg, writer, CandlePeriod2m)
	CreateCandles(ctx, twoMinuteCandlesChan, wg, writer, CandlePeriod10m)
}

func CreateCandles(ctx context.Context, inCandlesChan <-chan Candle, wg *sync.WaitGroup, writer *CandleWriter, period CandlePeriod) chan Candle {
	outCandlesChan := make(chan Candle)
	tickerCandles := map[string][]Candle{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(outCandlesChan)

		start, _ := <-inCandlesChan

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

		for {
			select {
			case candle, ok := <-inCandlesChan:
				if !ok {
					candles := parseCandles(period, tickerCandles)
					for _, newCandle := range candles {
						if period == CandlePeriod1m || period == CandlePeriod2m {
							outCandlesChan <- newCandle
						}
						writer.saveCandle(period, newCandle)
					}
					return
				}
				if candle.TS.Before(timeEnd) {
					tickerCandles[candle.Ticker] = append(tickerCandles[candle.Ticker], candle)
				} else {
					candles := parseCandles(period, tickerCandles)
					for _, newCandle := range candles {
						if period == CandlePeriod1m || period == CandlePeriod2m {
							outCandlesChan <- newCandle
						}
						writer.saveCandle(period, newCandle)
					}

					tickerCandles = map[string][]Candle{}

					newPeriodStart, _ := PeriodTS(period, candle.TS)
					timeEnd = newPeriodStart.Add(time.Minute * time.Duration(minuteCount))

					tickerCandles[candle.Ticker] = append(tickerCandles[candle.Ticker], candle)
				}
			case <-ctx.Done():
				candles := parseCandles(period, tickerCandles)
				for _, newCandle := range candles {
					if period == CandlePeriod1m || period == CandlePeriod2m {
						outCandlesChan <- newCandle
					}
					writer.saveCandle(period, newCandle)
				}
				return
			}
		}
	}()

	return outCandlesChan
}
