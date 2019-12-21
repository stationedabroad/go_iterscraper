package main

import (
	"flag"
	"fmt"
	"sync"
)

func main() {
	// Get flags
	urlBase := flag.String("url", "http://example.com/v/%d", `The URL you wish to scrape, containing "%d" where the id should be substituted`)
	idLow := flag.Int("from", 0, "The first ID that should be searched in the URL - inclusive.")
	idHigh := flag.Int("to", 1, "The last ID that should be searched in the URL - exclusive")
	concurrency := flag.Int("concurrency", 1, "How many scrapers to run in parallel. (More scrapers are faster, but more prone to rate limiting or bandwith issues)")
	outfile := flag.String("output", "output.csv", "Filename to export the CSV results")
	nameQuery := flag.String("nameQuery", ".name", "JQuery-style query for the name element")
	addressQuery := flag.String("addressQuery", ".address", "JQuery-style query for the address element")
	phoneQuery := flag.String("phoneQuery", ".phone", "JQuery-style query for the phone element")
	emailQuery := flag.String("emailQuery", ".email", "JQuery-style query for the email element")

	flag.Parse()

	// channel for emitting sites to fetch
	tasks := make(chan site)
	// Channel of data to write to disk
	results := make(chan site)

	// go emitTasks(taskChan, *urlBase, *idLow, *idHigh)
	go func() {
		for i := *idLow; i < *idHigh; i++ {
			url := fmt.Sprintf(*urlBase, i)
			tasks <- site{url: url, id: i}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(*concurrency)
	for i := 0; i < *concurrency; i++ {
		go func() {
			for {
				site := <-tasks
				site.fetch(*nameQuery, *addressQuery, *phoneQuery, *emailQuery)
				results <- site
			}
		}()
	}

	go writeSites(results, &wg, *outfile)

	wg.Wait()
}
