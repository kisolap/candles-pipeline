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
	File1 *csv.Writer
	File2 *csv.Writer
	File3 *csv.Writer
}

func NewCandleWriter(file1 io.Writer, file2 io.Writer, file3 io.Writer) *CandleWriter {
	return &CandleWriter{
		File1: csv.NewWriter(file1),
		File2: csv.NewWriter(file2),
		File3: csv.NewWriter(file3),
	}
}

func (w *CandleWriter) saveCandle(period CandlePeriod, candle Candle) {
	var file *csv.Writer
	switch period {
	case CandlePeriod1m:
		file = w.File1
	case CandlePeriod2m:
		file = w.File2
	case CandlePeriod10m:
		file = w.File3
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

func ProcessPrices(prices <-chan Price, wg *sync.WaitGroup, writer *CandleWriter) {
	wg.Add(1)
	outCandles := make(chan Candle)

	go func() {
		defer wg.Done()
		defer close(outCandles)
		for price := range prices {
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
		}
	}()

	minuteCandlesChan := CreateCandles(outCandles, wg, writer, CandlePeriod1m)
	twoMinuteCandlesChan := CreateCandles(minuteCandlesChan, wg, writer, CandlePeriod2m)
	CreateCandles(twoMinuteCandlesChan, wg, writer, CandlePeriod10m)
}

func CreateCandles(inCandlesChan <-chan Candle, wg *sync.WaitGroup, writer *CandleWriter, period CandlePeriod) chan Candle {
	outCandlesChan := make(chan Candle)
	tickerCandles := map[string][]Candle{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(outCandlesChan)

		start, ok := <-inCandlesChan
		if !ok {
			return
		}

		timeStart, err := PeriodTS(period, start.TS)
		if err != nil {
			err.Error()
		}

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
		}
	}()

	return outCandlesChan
}
