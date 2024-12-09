package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"spanner-perf-testing/database"
	"strconv"
	"sync"
	"time"
)

func main() {
	fmt.Println("Performance test starts...")
	//_ = os.Setenv("SPANNER_EMULATOR_HOST", "localhost:9010")

	type result struct {
		tenantID uint64
		count    int
		duration time.Duration
	}
	var results []result
	var wg sync.WaitGroup
	var mu sync.Mutex
	sem := make(chan struct{}, 10)

	for i := 10000; i < 20000; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func(tenantID uint64) {
			defer wg.Done()
			start := time.Now()
			lastPolicyID := int64(-1)
			totalCount := 0
			for {
				count, nextToken, err := database.QueryByPrimaryKey(tenantID, lastPolicyID)
				//count, nextToken, err := database.QueryByTenantID(tenantID, lastPolicyID)
				if err != nil {
					panic(err)
				}
				totalCount += count

				if nextToken == -1 {
					break
				}
				lastPolicyID = nextToken
			}
			duration := time.Since(start)

			mu.Lock()
			results = append(results, result{tenantID: tenantID, count: totalCount, duration: duration})
			mu.Unlock()
			<-sem
		}(uint64(i + 1))
	}

	wg.Wait()

	file, err := os.Create("pk_private_app_prod_no_page.csv")
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = file.Close()
	}()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write CSV header
	_ = writer.Write([]string{"Tenant ID", "Count", "Duration"})

	// Write test results
	for _, r := range results {
		if err := writer.Write([]string{strconv.FormatUint(r.tenantID, 10), strconv.FormatInt(int64(r.count), 10), strconv.FormatFloat(r.duration.Seconds(), 'g', -1, 64)}); err != nil {
			log.Fatalf("Failed to write record: %v", err)
		}
	}
	fmt.Println("Performance test completed!")
}
