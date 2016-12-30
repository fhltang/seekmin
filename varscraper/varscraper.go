// Tool to periodically scrape /debug/vars on the provided target stats
// to a file.

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

var configStr = flag.String("config", "", "Configuration.  json.Marshal version of main.Config.")
var configFile = flag.String("config_file", "", "File from which to read configuration.")

type Config struct {
	// Configuration for monitoring target.
	Target TargetConfig

	// Configuration for output.
	Output OutputConfig
}

type TargetConfig struct {
	// Monitoring target, typically in host:port format, e.g. localhost:8080.
	Target string

	// Sampling period.  Determines how frequently metrics are sampled from the target.
	PeriodMs int64
}

type OutputConfig struct {
	// Prefix for output filenames.
	FilenamePrefix string

	// Records to write before creating a new file.
	MaxRecordsPerFile int

	// List of variables that should be in the output.
	Vars []string
}

type Scraper struct {
	config Config

	record chan string
}

// Like time.Tick() except we "tick" immediately before waiting period.
func TickNowAndForever(period time.Duration) <-chan time.Time {
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
	for {
		// filename is only used if this.config.Output.FilenamePrefix != ""
		now := time.Now()
		filename := fmt.Sprintf(
			"%s%s.csv",
			this.config.Output.FilenamePrefix,
			now.Format("20060102-150405.999"))

		func() {
			var f io.Writer
			if this.config.Output.FilenamePrefix != "" {
				log.Printf("Creating output file %s", filename)
				file, err := os.Create(filename)
				if err != nil {
					log.Fatalf("Failed to open %s for writing: %s", filename, err)
				}
				defer file.Close()
				f = file
			} else {
				f = os.Stdout
			}

			recordsAppended := 0

			header := bytes.NewBufferString("")
			header.WriteString("timestamp")
			for _, col := range this.config.Output.Vars {
				header.WriteString(fmt.Sprintf(",%s", col))
			}
			header.WriteString("\n")
			f.Write(header.Bytes())

			for rec := range this.record {
				f.Write([]byte(fmt.Sprintf("%s\n", rec)))
				recordsAppended++
				if recordsAppended > this.config.Output.MaxRecordsPerFile {
					return
				}
			}
		}()
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
	rec := bytes.NewBufferString(t.Format(time.RFC3339Nano))
	defer func() { this.record <- rec.String() }()


	url := fmt.Sprintf("http://%s/debug/vars", this.config.Target.Target)
	resp, err := http.Get(url)
	if err != nil {
		log.Printf("Failed to GET %s", url)
		for _, _ = range this.config.Output.Vars {
			rec.WriteString(",")
		}
		return
	}

	vars, err := ioutil.ReadAll(resp.Body)

	var f interface{}
	err = json.Unmarshal(vars, &f)
	m := f.(map[string]interface{})
	for _, k := range this.config.Output.Vars {
		rec.WriteString(",")
		if v, ok := m[k]; ok {
			switch vv := v.(type) {
			case map[string]interface{}:
				for kk, vvv := range vv {
					rec.WriteString(fmt.Sprint(kk, "=", vvv))
				}
			default:
				rec.WriteString(fmt.Sprint(vv))
			}
		}
	}

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
