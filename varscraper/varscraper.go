// Tool to periodically scrape /debug/vars on the provided target stats
// to a file.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

var target = flag.String(
	"target", "",
	"Host:port of HTTP server which exports /debug/vars")
var periodMs = flag.Int(
	"period_ms", 1000,
	"Polling period in milliseconds.")
var varWhitelist = StringList{}

func init() {
	flag.Var(
		&varWhitelist, "var_whitelist",
		"Comma-separated list of vars that should be collected.")
}

type StringList struct {
	Values []string
}

func (this *StringList) String() string {
	return strings.Join(this.Values, ",")
}

func (this *StringList) Set(value string) error {
	this.Values = strings.Split(value, ",")
	return nil
}

type Scraper struct {
	target string
	period time.Duration
	ticker *time.Ticker
	whitelist []string
}

func NewScraper(target string, period time.Duration, whitelist []string) *Scraper {
	ticker := time.NewTicker(period)
	scraper := &Scraper{
		target: target,
		period: period,
		ticker: ticker,
		whitelist: whitelist,
	}
	return scraper
}

func (this *Scraper) StartAndWait() {
	for {
		select {
		case t := <-this.ticker.C:
			this.doScrape(t)
		}
	}
}

func (this *Scraper) doScrape(t time.Time) {
	url := fmt.Sprintf("http://%s/debug/vars", this.target)
	resp, err := http.Get(url)
	if err != nil {
		log.Printf("Failed to GET %s", url)
		return
	}

	vars, err := ioutil.ReadAll(resp.Body)

	fmt.Print(t.UnixNano())
	
	var f interface{}
	err = json.Unmarshal(vars, &f)
	m := f.(map[string]interface{})
	for _, k := range this.whitelist {
		if v, ok := m[k]; ok {
			switch vv:=v.(type) {
			case map[string]interface{}:
				for kk, vvv := range vv {
					fmt.Print(",", kk, "=", vvv)
				}
			default:
				fmt.Print(",", vv)
			}
		} else {
			fmt.Print(",")
		}
	}
	
	fmt.Println()
}

func main() {
	flag.Parse()

	if *target == "" {
		log.Fatal("Must specify monitoring target using --target.")
	}

	scraper := NewScraper(
		*target,
		time.Duration(*periodMs)*time.Millisecond,
		varWhitelist.Values)
	scraper.StartAndWait()
}
