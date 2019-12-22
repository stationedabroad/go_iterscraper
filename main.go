package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/PuerkitoBio/goquery"
)

func main() {
	urlBase := flag.String("url", "http://example.com/v/%d", `The URL you wish to scrape, containing "%d" where the id should be substituted`)
	idLow := flag.Int("from", 0, "The first ID that should be searched in the URL - inclusive.")
	idHigh := flag.Int("to", 1, "The last ID that should be searched in the URL - exclusive")
	concurrency := flag.Int("concurrency", 1, "How many scrapers to run in parallel. (More scrapers are faster, but more prone to rate limiting or bandwith issues)")
	outfile := flag.String("output", "output.csv", "Filename to export the CSV results")
	name := flag.String("nameQuery", ".name", "JQuery-style query for the name element")
	address := flag.String("addressQuery", ".address", "JQuery-style query for the address element")
	phone := flag.String("phoneQuery", ".phone", "JQuery-style query for the phone element")
	email := flag.String("emailQuery", ".email", "JQuery-style query for the email element")

	flag.Parse()

	columns := []string{*name, *address, *phone, *email}
	headers := []string{"name", "address", "phone", "email"}
	headers = append([]string{"url", "id"}, headers...)

	type task struct {
		url string
		id  int
	}
	tasks := make(chan task)
	go func() {
		for i := *idLow; i < *idHigh; i++ {
			tasks <- task{url: fmt.Sprintf(*urlBase, i), id: i}
		}
		close(tasks)
	}()

	var wg sync.WaitGroup
	wg.Add(*concurrency)
	results := make(chan []string)
	go func() {
		wg.Wait()
		close(results)
	}()

	for i := 0; i < *concurrency; i++ {
		go func() {
			for t := range tasks {
				site, err := fetch(t.url, t.id, columns)
				if err != nil {
					log.Printf("Could not fetch url and id: %v", err)
					continue
				}
				results <- site
			}
			wg.Done()
		}()
	}

	err := writeSites(results, *outfile, headers)
	if err != nil {
		log.Printf("could not dump CSV: %v", err)
	}
}

func fetch(url string, id int, queries []string) ([]string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("Could not read url %s: error %v", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusTooManyRequests {
			return nil, fmt.Errorf("Rate limiting detected")
		}
		return nil, fmt.Errorf("bad response from server: %s", resp.Status)
	}

	// Load response into GoQuery
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("could not parse %s: %v", url, err)
	}

	s := []string{url, strconv.Itoa(id)}

	for _, q := range queries {
		s = append(s, strings.TrimSpace(doc.Find(q).Text()))
	}
	return s, nil
}

func writeSites(results chan []string, outfile string, headers []string) error {
	file, err := os.Create(outfile)
	if err != nil {
		return fmt.Errorf("Unable to open file %s - error %v", outfile, err)
	}

	defer file.Close()

	w := csv.NewWriter(file)
	defer w.Flush()

	if err := w.Write(headers); err != nil {
		return fmt.Errorf("error writing record to csv: %v", err)
	}

	for s := range results {
		if err := w.Write(s); err != nil {
			return fmt.Errorf("error writing record to csv: %v", err)
		}
	}
	if err := w.Error(); err != nil {
		return fmt.Errorf("could not write file: %v", err)
	}

	return nil
}
