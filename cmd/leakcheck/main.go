// Command leakcheck alternates two requests through the pooled handler and
// asserts that no byte of request A ever appears in request B's response.
package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"runtime/debug"
	"strings"

	"poolleak"
)

func main() {
	rounds := flag.Int("rounds", 1000, "number of alternating request pairs")
	flag.Parse()

	debug.SetGCPercent(-1) // keep pooled objects hot, like a saturated service
	defer runtime.GC()

	h := poolleak.NewHandler()
	srv := httptest.NewServer(h)
	defer srv.Close()
	client := srv.Client()

	leaks := 0
	for i := 0; i < *rounds; i++ {
		marker := fmt.Sprintf("MARKER_A_%d", i)
		urlA := srv.URL + "/pay?user=alice&amount=10000&tags=" + marker +
			"&notes=" + marker + "&size=4096"
		if body, _, err := get(client, urlA); err != nil {
			fmt.Fprintf(os.Stderr, "request A failed on round %d: %v\n", i, err)
			os.Exit(2)
		} else if !strings.Contains(body, marker) {
			fmt.Fprintf(os.Stderr, "sanity check failed on round %d: marker missing from A: %q\n", i, body)
			os.Exit(2)
		}

		urlB := srv.URL + "/pay?user=bob&amount=7"
		body, headers, err := get(client, urlB)
		if err != nil {
			fmt.Fprintf(os.Stderr, "request B failed on round %d: %v\n", i, err)
			os.Exit(2)
		}

		bad := []string{}
		if strings.Contains(body, "MARKER_A") || strings.Contains(body, "alice") {
			bad = append(bad, "body="+scrub(body))
		}
		for k, vals := range headers {
			for _, v := range vals {
				if strings.Contains(k, "MARKER_A") || strings.Contains(v, "MARKER_A") ||
					strings.Contains(v, "alice") {
					bad = append(bad, k+"="+v)
				}
			}
		}
		if len(bad) > 0 {
			leaks++
			if leaks <= 5 {
				fmt.Printf("round %d LEAK: %s\n", i, strings.Join(bad, "; "))
			}
		}
	}

	if leaks > 0 {
		fmt.Printf("FAIL: %d/%d rounds leaked previous-request data\n", leaks, *rounds)
		os.Exit(1)
	}
	fmt.Printf("OK: %d rounds, zero cross-request leakage\n", *rounds)
}

func get(client *http.Client, url string) (string, http.Header, error) {
	resp, err := client.Get(url)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", nil, err
	}
	return string(data), resp.Header, nil
}

func scrub(s string) string {
	if len(s) > 120 {
		return s[:120] + "...(truncated)"
	}
	return s
}
