package main

import (
	"fmt"
	"github.com/urfave/cli"
)

func metricsTotalReqCount(c *cli.Context) error {
	client := getClient(c.GlobalString("server"))

	url := c.String("url")
	method := c.String("method")
	window := c.String("window")
	fn := c.String("function")
	ns := "default"

	result, err := client.TotalRequestToUrlGet(url, method, window, fn, ns)
	checkErr(err, "get metricsTotalReqCount")

	fmt.Println(result)
	return err
}

func metricsTotalErrorCount(c *cli.Context) error {
	client := getClient(c.GlobalString("server"))

	fn := c.String("function")
	ns := "default"
	url := c.String("url")
	window := c.String("window")
	method := c.String("method")

	result, err := client.TotalErrorRequestToFuncGet(fn, ns, window, url, method)
	checkErr(err, "get metricsTotalReqCount")

	fmt.Println(result)
	return err
}
