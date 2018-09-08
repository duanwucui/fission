package canaryconfigmgr

import (
	"fmt"
	"time"
	"golang.org/x/net/context"

	promClient "github.com/prometheus/client_golang/api/prometheus"
	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
)

type PrometheusApiClient struct {
	client promClient.QueryAPI
}

func MakePrometheusClient(prometheusSvc string) *PrometheusApiClient {
	log.Printf("Making prom client with service : %s", prometheusSvc)
	promApiConfig := promClient.Config{
		Address: prometheusSvc,
	}

	promApiClient, err := promClient.New(promApiConfig)
	if err != nil {
		log.Errorf("Error creating prometheus api client for svc : %s, err : %v", prometheusSvc, err)
	}

	apiQueryClient := promClient.NewQueryAPI(promApiClient)

	log.Printf("Successfully made prom client")
	return &PrometheusApiClient{
		client: apiQueryClient,
	}
}

func(promApiClient *PrometheusApiClient) GetFunctionFailurePercentage(path, method, funcName, funcNs string, window string) (float64, error) {
	// first get a total count of requests to this url in a time window
	reqs, err := promApiClient.GetRequestsToFuncInWindow(path, method, funcName, funcNs, window)
	if err != nil {
	   return 0, err
	}

	if reqs == 0 {
	   return -1, fmt.Errorf("no requests to this url %v and method %v in the window : %v", path, method, window)
	}

	// next, get a total count of errored out requests to this function in the same window
	failedReqs, err := promApiClient.GetTotalFailedRequestsToFuncInWindow(funcName, funcNs, path, method, window)
	if err != nil {
	   return 0, err
	}

	// calculate the failure percentage of the function
	failurePercentForFunc := (reqs / failedReqs) * 100
	log.Printf("Final failurePercentForFunc for func: %v.%v is %v", funcName, funcNs, failurePercentForFunc)

	return failurePercentForFunc, nil
}

func(promApiClient *PrometheusApiClient) GetRequestsToFuncInWindow(path string, method string, funcName string, funcNs string, window string) (float64, error) {
	queryString := fmt.Sprintf("fission_function_calls_total{path=\"%s\",method=\"%s\",name=\"%s\",namespace=\"%s\"}[%v]", path, method, funcName, funcNs, window)
	log.Printf("Querying total function calls for : %s ", queryString)

	reqs, err := promApiClient.executeQuery(queryString)
	if err != nil {
		log.Printf("Error executing query : %s, err : %v", queryString, err)
		return 0, err
	}

	queryString = fmt.Sprintf("fission_function_calls_total{path=\"%s\",method=\"%s\",name=\"%s\",namespace=\"%s\"}[%v]", path, method, funcName, funcNs, addInterval(window))
	log.Printf("Querying total function calls for : %s ", queryString)

	reqsInPrevWindow, err := promApiClient.executeQuery(queryString)
	if err != nil {
		log.Printf("Error executing query : %s, err : %v", queryString, err)
		return 0, err
	}

	reqsInCurrentWindow := reqs - reqsInPrevWindow

	log.Printf("total calls to this function %v method %v : %v", path, method, reqsInCurrentWindow)

	return reqsInCurrentWindow, nil
}

func (promApiClient *PrometheusApiClient) GetTotalFailedRequestsToFuncInWindow(funcName string, funcNs string, path string, method string, window string) (float64, error) {
	queryString := fmt.Sprintf("fission_function_errors_total{name=\"%s\",namespace=\"%s\",path=\"%s\", method=\"%s\"}[%v]", funcName, funcNs, path, method, window)
	log.Printf("Querying fission_function_errors_total qs : %s", queryString)

	failedRequests, err := promApiClient.executeQuery(queryString)
	if err != nil {
		log.Printf("Error executing query : %s, err : %v", queryString, err)
		return 0, err
	}

	queryString = fmt.Sprintf("fission_function_errors_total{name=\"%s\",namespace=\"%s\",path=\"%s\", method=\"%s\"}[%v]", funcName, funcNs, path, method, addInterval(window))
	log.Printf("Querying fission_function_errors_total qs : %s", queryString)

	failedReqsInPrevWindow, err := promApiClient.executeQuery(queryString)
	if err != nil {
		log.Printf("Error executing query : %s, err : %v", queryString, err)
		return 0, err
	}

	failedReqsInCurrentWindow := failedRequests - failedReqsInPrevWindow
	log.Printf("total failed calls to function: %v.%v : %v", funcName, funcNs, failedReqsInCurrentWindow)

	return failedReqsInCurrentWindow, nil
}

func(promApiClient *PrometheusApiClient) executeQuery(queryString string) (float64, error) {
	val, err := promApiClient.client.Query(context.Background(), queryString, time.Now())
	if err != nil {
		log.Errorf("Error querying prometheus qs : %v, err : %v", queryString, err)
		return 0, err
	}

	log.Printf("Value retrieved from query : %v", val)

	switch {
	case val.Type() == model.ValScalar:
		log.Printf("Value type is scalar")
		scalarVal := val.(*model.Scalar)
		log.Printf("scalarValue : %v", scalarVal.Value)
		return float64(scalarVal.Value), nil

	case val.Type() == model.ValVector:
		log.Printf("value type is vector")
		vectorVal := val.(model.Vector)
		total := float64(0)
		for _, elem := range vectorVal {
			log.Printf("labels : %v, Elem value : %v, timestamp : %v", elem.Metric, elem.Value, elem.Timestamp)
			total = total + float64(elem.Value)
		}
		return total, nil

	case val.Type() == model.ValMatrix:
		//log.Printf("value type is matrix")
		matrixVal := val.(model.Matrix)
		total := float64(0)
		for _, elem := range matrixVal {
				//log.Printf("Only one value, so taking the 0th elem")
				total += float64(elem.Values[len(elem.Values)-1].Value)
		}
		log.Printf("Final total : %v", total)
		return total, nil

	default:
		log.Printf("type unrecognized")
		return 0,nil
	}
}

func addInterval(window string) string {
	timeDuration, _ := time.ParseDuration(window)
	log.Println("window in seconds", int64(timeDuration/time.Second))

	timeInStr := fmt.Sprintf("%ds", int64((timeDuration + timeDuration)/time.Second))
	fmt.Println(timeInStr)

	return timeInStr
}