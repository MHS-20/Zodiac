package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/MHS-20/Zodiac/kvclient"
)

func main() {
	var (
		addrs    = flag.String("addr", "localhost:8000", "comma-separated server addresses")
		discover = flag.Bool("discover", false, "discover cluster members from seed addresses")
		timeout  = flag.Duration("timeout", 5*time.Second, "request timeout")
	)
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "Usage: zodiac-client [flags] <command> [args...]\n\nCommands:\n  get <key>\n  put <key> <value>\n  append <key> <value>\n  cas <key> <compare> <value>\n  list <prefix>\n")
		os.Exit(1)
	}

	cmd := flag.Arg(0)
	args := flag.Args()[1:]

	var c *kvclient.KVClient
	seedAddrs := splitAddrs(*addrs)
	if *discover {
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()
		c = kvclient.NewWithDiscovery(ctx, seedAddrs)
	} else {
		c = kvclient.New(seedAddrs)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch cmd {
	case "get":
		if len(args) < 1 {
			log.Fatal("Usage: zodiac-client get <key>")
		}
		val, found, err := c.Get(ctx, args[0])
		if err != nil {
			log.Fatal(err)
		}
		if !found {
			os.Exit(1)
		}
		fmt.Println(val)

	case "put":
		if len(args) < 2 {
			log.Fatal("Usage: zodiac-client put <key> <value>")
		}
		prev, found, err := c.Put(ctx, args[0], args[1])
		if err != nil {
			log.Fatal(err)
		}
		if found {
			fmt.Println(prev)
		}

	case "append":
		if len(args) < 2 {
			log.Fatal("Usage: zodiac-client append <key> <value>")
		}
		prev, found, err := c.Append(ctx, args[0], args[1])
		if err != nil {
			log.Fatal(err)
		}
		if found {
			fmt.Println(prev)
		}

	case "cas":
		if len(args) < 3 {
			log.Fatal("Usage: zodiac-client cas <key> <compare> <value>")
		}
		prev, found, err := c.CAS(ctx, args[0], args[1], args[2])
		if err != nil {
			log.Fatal(err)
		}
		if found {
			fmt.Println(prev)
		}

	case "list":
		if len(args) < 1 {
			log.Fatal("Usage: zodiac-client list <prefix>")
		}
		pairs, err := c.List(ctx, args[0])
		if err != nil {
			log.Fatal(err)
		}
		for k, v := range pairs {
			fmt.Printf("%s: %s\n", k, v)
		}

	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		os.Exit(1)
	}
}

func splitAddrs(s string) []string {
	var addrs []string
	for start := 0; start < len(s); {
		end := start
		for end < len(s) && s[end] != ',' {
			end++
		}
		if addr := s[start:end]; addr != "" {
			addrs = append(addrs, addr)
		}
		start = end + 1
	}
	return addrs
}
