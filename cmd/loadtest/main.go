package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"auction-platform/internal/auction"
	"auction-platform/internal/auth"
)

type sample struct {
	status  int
	latency time.Duration
}

type result struct {
	Profile      string          `json:"profile"`
	TargetRPS    int             `json:"target_rps"`
	Seconds      float64         `json:"seconds"`
	Scheduled    int             `json:"scheduled"`
	Dropped      int             `json:"generator_dropped"`
	Completed    int             `json:"completed"`
	Success      int             `json:"success"`
	SuccessRPS   float64         `json:"success_rps"`
	Statuses     map[int]int     `json:"statuses"`
	P95          float64         `json:"p95_ms"`
	P99          float64         `json:"p99_ms"`
	SuccessP95   float64         `json:"success_p95_ms"`
	SuccessP99   float64         `json:"success_p99_ms"`
	Pass         bool            `json:"pass"`
	StatusBefore json.RawMessage `json:"status_before"`
	StatusAfter  json.RawMessage `json:"status_after"`
}

type runner struct {
	client      *http.Client
	base, token string
	lots        []string
	amounts     []int64
}

func (r *runner) request(method, path, token string, body any, out any) error {
	data, _ := json.Marshal(body)
	req, err := http.NewRequest(method, r.base+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 2048))
		return fmt.Errorf("%s %s: %d %s", method, path, res.StatusCode, b)
	}
	if out != nil {
		return json.NewDecoder(res.Body).Decode(out)
	}
	_, err = io.Copy(io.Discard, res.Body)
	return err
}

func (r *runner) status() json.RawMessage {
	var v json.RawMessage
	if err := r.request("GET", "/status", "", nil, &v); err != nil {
		return json.RawMessage(`{"unavailable":true}`)
	}
	return v
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	return values[max(0, int(math.Ceil(float64(len(values))*p))-1)]
}

func (r *runner) stage(profile string, rate int, duration time.Duration) result {
	out := result{Profile: profile, TargetRPS: rate, Statuses: map[int]int{}, StatusBefore: r.status()}
	jobs := make(chan struct{})
	samples := make(chan sample, 1024)
	var wg sync.WaitGroup
	var ready sync.WaitGroup
	ready.Add(len(r.lots))
	for i := range r.lots {
		wg.Go(func() {
			ready.Done()
			for range jobs {
				method, path, body := "GET", "/api/v1/auctions/"+r.lots[i], []byte(nil)
				if profile == "bids" {
					method = "POST"
					path += "/bids"
					r.amounts[i] += 100
					body = []byte(fmt.Sprintf(`{"amount":%d}`, r.amounts[i]))
				}
				req, _ := http.NewRequest(method, r.base+path, bytes.NewReader(body))
				if profile == "bids" {
					req.Header.Set("Content-Type", "application/json")
					req.Header.Set("Authorization", "Bearer "+r.token)
					req.Header.Set("Idempotency-Key", uuid.NewString())
				}
				start := time.Now()
				res, err := r.client.Do(req)
				code := 0
				if err == nil {
					code = res.StatusCode
					_, readErr := io.Copy(io.Discard, res.Body)
					res.Body.Close()
					if readErr != nil {
						code = 0
					}
				}
				samples <- sample{status: code, latency: time.Since(start)}
			}
		})
	}
	var values, successful []float64
	collected := make(chan struct{})
	go func() {
		defer close(collected)
		for s := range samples {
			out.Completed++
			out.Statuses[s.status]++
			ms := float64(s.latency) / float64(time.Millisecond)
			values = append(values, ms)
			if s.status >= 200 && s.status < 300 {
				out.Success++
				successful = append(successful, ms)
			}
		}
	}()
	ready.Wait()
	start := time.Now().Add(20 * time.Millisecond)
	total := int(duration.Seconds() * float64(rate))
	for n := 0; n < total; n++ {
		target := start.Add(time.Duration(float64(n) * float64(time.Second) / float64(rate)))
		if delay := time.Until(target); delay > 0 {
			time.Sleep(delay)
		}
		out.Scheduled++
		select {
		case jobs <- struct{}{}:
		default:
			out.Dropped++
		}
	}
	close(jobs)
	wg.Wait()
	close(samples)
	<-collected
	// Include the full scheduled interval, even if the last response arrives early.
	if remaining := time.Until(start.Add(duration)); remaining > 0 {
		time.Sleep(remaining)
	}
	out.Seconds = time.Since(start).Seconds()
	out.SuccessRPS = float64(out.Success) / out.Seconds
	out.P95 = percentile(values, .95)
	out.P99 = percentile(values, .99)
	out.SuccessP95 = percentile(successful, .95)
	out.SuccessP99 = percentile(successful, .99)
	out.Pass = out.Dropped == 0 && out.Completed > 0 && float64(out.Success)/float64(out.Completed) >= .99 && out.P99 <= 250 && out.SuccessRPS >= float64(rate)*.98
	out.StatusAfter = r.status()
	return out
}

func run() error {
	base := flag.String("base-url", "http://127.0.0.1:8080", "local development API URL")
	ratesRaw := flag.String("rates", "50,100,200,400,800", "offered RPS stages")
	seconds := flag.Duration("duration", 15*time.Second, "duration of each stage")
	workers := flag.Int("workers", 32, "maximum concurrent requests (one auction per worker)")
	profilesRaw := flag.String("profiles", "reads,bids", "reads,bids or either profile")
	output := flag.String("output", "load-results.json", "JSON report")
	confirm := flag.Duration("confirm", 60*time.Second, "repeat highest passing rate per profile; 0 disables")
	flag.Parse()
	if *workers < 1 || *workers > 256 || *seconds < time.Second || *confirm < 0 {
		return fmt.Errorf("invalid worker count or duration")
	}
	var rates []int
	for _, raw := range strings.Split(*ratesRaw, ",") {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 10000 {
			return fmt.Errorf("invalid rate")
		}
		rates = append(rates, n)
	}
	profiles := strings.Split(*profilesRaw, ",")
	for _, p := range profiles {
		if p != "reads" && p != "bids" {
			return fmt.Errorf("invalid profile")
		}
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = 256
	tr.MaxIdleConnsPerHost = 256
	r := &runner{client: &http.Client{Transport: tr, Timeout: 5 * time.Second}, base: strings.TrimRight(*base, "/")}
	defer tr.CloseIdleConnections()
	var tokens []string
	for range 2 {
		c := auth.Credentials{Email: "load-" + uuid.NewString() + "@example.com", Password: uuid.NewString()}
		if err := r.request("POST", "/api/v1/auth/register", "", c, nil); err != nil {
			return err
		}
		var s auth.Session
		if err := r.request("POST", "/api/v1/auth/login", "", c, &s); err != nil {
			return err
		}
		tokens = append(tokens, s.Token)
	}
	defer func() {
		for _, token := range tokens {
			_ = r.request("POST", "/api/v1/auth/logout", token, nil, nil)
		}
	}()
	r.token = tokens[1]
	for i := 0; i < *workers; i++ {
		var a auction.Auction
		input := auction.CreateInput{Title: "Load test " + uuid.NewString(), StartingPrice: 1000, MinIncrement: 100, StartsAt: time.Now().Add(-time.Minute), EndsAt: time.Now().Add(6 * time.Hour)}
		if err := r.request("POST", "/api/v1/auctions", tokens[0], input, &a); err != nil {
			return err
		}
		r.lots = append(r.lots, a.ID.String())
		r.amounts = append(r.amounts, 900)
	}
	report := struct {
		Started      time.Time `json:"started"`
		Go, OS, Arch string
		Workers      int
		AuctionIDs   []string `json:"auction_ids"`
		Results      []result `json:"results"`
	}{Started: time.Now().UTC(), Go: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH, Workers: *workers, AuctionIDs: r.lots}
	save := func() error {
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return err
		}
		return os.WriteFile(*output, append(data, '\n'), 0644)
	}
	for _, profile := range profiles {
		best := 0
		for _, rate := range rates {
			v := r.stage(profile, rate, *seconds)
			report.Results = append(report.Results, v)
			fmt.Printf("%s target=%d success=%.1f RPS p95=%.2fms p99=%.2fms dropped=%d statuses=%v pass=%v\n", profile, rate, v.SuccessRPS, v.P95, v.P99, v.Dropped, v.Statuses, v.Pass)
			if err := save(); err != nil {
				return err
			}
			if v.Pass && rate > best {
				best = rate
			}
		}
		if best > 0 && *confirm > 0 {
			v := r.stage(profile, best, *confirm)
			report.Results = append(report.Results, v)
			fmt.Printf("%s confirmation target=%d success=%.1f RPS p95=%.2fms p99=%.2fms dropped=%d statuses=%v pass=%v\n", profile, best, v.SuccessRPS, v.P95, v.P99, v.Dropped, v.Statuses, v.Pass)
			if err := save(); err != nil {
				return err
			}
		}
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
