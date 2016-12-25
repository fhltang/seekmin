// Tool to periodically scrape /debug/vars on the provided target stats
// to a file.

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"io/ioutil"
	"net/http"
	"time"
)

var configStr = flag.String("config", "", "Configuration.  json.Marshal version of main.Config.")
var configFile = flag.String("config_file", "", "File from which to read configuration.")

type Config struct {
	Target TargetConfig
	Output OutputConfig
}

type TargetConfig struct {
	Target string
	PeriodMs int64
	Vars []string
}

type OutputConfig struct {
	// None as yet.  Output is CSV format to stdout.
}

type Scraper struct {
	config Config

	record chan string
}

// Like time.Tick() except we "tick" immediately before waiting period.
func TickNowAndForever(period time.Duration) <-chan time.Time{
	ticks := make(chan time.Time)
	go func() {
		ticks <- time.Now()
		c := time.Tick(period)
		for t := range c {
			ticks <- t
		}
	}()
	
	return ticks
}

func NewScraper(config Config) *Scraper {
	return &Scraper{config: config, record: make(chan string)}
}

func (this *Scraper) WriteRecords() {
	fmt.Print("timestamp")
	for _, col := range this.config.Target.Vars {
		fmt.Printf(",%s", col)
	}
	fmt.Println()
	for rec := range this.record {
		fmt.Println(rec)
	}
}

func (this *Scraper) StartAndWait() {
	go this.WriteRecords()
	c := TickNowAndForever(time.Duration(this.config.Target.PeriodMs) * time.Millisecond)
	for t := range c {
		this.doScrape(t)
	}
}

func (this *Scraper) doScrape(t time.Time) {
	targetConfig := this.config.Target
	
	url := fmt.Sprintf("http://%s/debug/vars", targetConfig.Target)
	resp, err := http.Get(url)
	if err != nil {
		log.Printf("Failed to GET %s", url)
		return
	}

	vars, err := ioutil.ReadAll(resp.Body)

	rec := bytes.NewBufferString("")

	rec.WriteString(t.Format(time.RFC3339Nano))
	
	var f interface{}
	err = json.Unmarshal(vars, &f)
	m := f.(map[string]interface{})
	for _, k := range targetConfig.Vars {
		rec.WriteString(",")
		if v, ok := m[k]; ok {
			switch vv:=v.(type) {
			case map[string]interface{}:
				for kk, vvv := range vv {
					rec.WriteString(fmt.Sprint(kk, "=", vvv))
				}
			default:
				rec.WriteString(fmt.Sprint(vv))
			}
		}
	}
	
	this.record <- rec.String()
}

func main() {
	flag.Parse()

	var config Config
	var configBytes []byte
	var err error

	if *configStr != "" && *configFile != "" {
		log.Fatal("Exactly one of --config or --config_file must be specified")
	}

	if *configFile != "" {
		configBytes, err = ioutil.ReadFile(*configFile)
		if err != nil {
			log.Fatalf("Failed reading config file %s: %s", *configFile, err)
		}
	} else {
		configBytes = []byte(*configStr)
	}

	err = json.Unmarshal(configBytes, &config)
	if err != nil {
		log.Fatalf("Failed to parse config %s: %s", *configStr, err)
	}

	if config.Target.Target == "" {
		log.Fatal("Must specify monitoring target in config.")
	}

	scraper := NewScraper(config)
	scraper.StartAndWait()
}
