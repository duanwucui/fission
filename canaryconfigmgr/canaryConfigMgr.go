/*
Copyright 2016 The Fission Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package canaryconfigmgr

import (
	log "github.com/sirupsen/logrus"
	"context"
	"time"
	"os"

	k8sCache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/kubernetes"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/apimachinery/pkg/fields"

	"github.com/fission/fission/crd"
	"fmt"
)

type canaryConfigMgr struct {
	fissionClient     		  *crd.FissionClient
	kubeClient        		  *kubernetes.Clientset
	canaryConfigStore         k8sCache.Store
	canaryConfigController    k8sCache.Controller
	promClient 				  *PrometheusApiClient
	crdClient         		  *rest.RESTClient
	canaryCfgCancelFuncMap 	  *canaryConfigCancelFuncMap
}

func MakeCanaryConfigMgr(fissionClient *crd.FissionClient, kubeClient *kubernetes.Clientset, crdClient *rest.RESTClient) (*canaryConfigMgr, error) {

	// TODO : Change this to use k8s service.
	prometheusSvc := os.Getenv("PROMETHEUS_SERVER_SERVICE_HOST")
	if prometheusSvc == "" {
		log.Printf("Error finding prometheus Service, Env : %v", os.Environ())
		return nil, fmt.Errorf("prometheus service not found, cant create canary deployments")
	}

	configMgr := &canaryConfigMgr{
		fissionClient: fissionClient,
		kubeClient: kubeClient,
		crdClient: crdClient,
		promClient: MakePrometheusClient(fmt.Sprintf("http://%s", prometheusSvc)),
		canaryCfgCancelFuncMap: makecanaryConfigCancelFuncMap(),
	}

	store, controller := configMgr.initCanaryConfigController()
	configMgr.canaryConfigStore = store
	configMgr.canaryConfigController = controller

	return configMgr, nil
}

// TODO : If fn updated once canary config is processed completely, we probably have to restart processing the corresponding canary config
// That requires us to have a func watcher that basically calls `addCanaryConfig` in the occurrance of such an event.
func(canaryCfgMgr *canaryConfigMgr) initCanaryConfigController() (k8sCache.Store, k8sCache.Controller) {
	resyncPeriod := 30 * time.Second
	listWatch := k8sCache.NewListWatchFromClient(canaryCfgMgr.crdClient, "canaryconfigs", metav1.NamespaceAll, fields.Everything())
	store, controller := k8sCache.NewInformer(listWatch, &crd.CanaryConfig{}, resyncPeriod,
		k8sCache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				canaryConfig := obj.(*crd.CanaryConfig)
				go canaryCfgMgr.addCanaryConfig(canaryConfig)
			},
			DeleteFunc: func(obj interface{}) {
				canaryConfig := obj.(*crd.CanaryConfig)
				go canaryCfgMgr.deleteCanaryConfig(canaryConfig)
			},
			UpdateFunc: func(oldObj interface{}, newObj interface{}) {
				oldConfig := oldObj.(*crd.CanaryConfig)
				newConfig := newObj.(*crd.CanaryConfig)
				if oldConfig.Metadata.ResourceVersion != newConfig.Metadata.ResourceVersion {
					go canaryCfgMgr.updateCanaryConfig(oldConfig, newConfig)
				}
				go canaryCfgMgr.reSyncCanaryConfigs()

			},
		})

	return store, controller
}

func (canaryCfgMgr *canaryConfigMgr) Run(ctx context.Context) {
	go canaryCfgMgr.canaryConfigController.Run(ctx.Done())
	log.Printf("started Canary configmgr controller")
}

func(canaryCfgMgr *canaryConfigMgr) addCanaryConfig(canaryConfig *crd.CanaryConfig) {
	log.Printf("addCanaryConfig called for %s", canaryConfig.Metadata.Name)
	ctx, cancel := context.WithCancel(context.Background())
	canaryCfgMgr.canaryCfgCancelFuncMap.assign(&canaryConfig.Metadata, &cancel)
	canaryCfgMgr.processCanaryConfig(&ctx, canaryConfig)
}

func(canaryCfgMgr *canaryConfigMgr) processCanaryConfig(ctx *context.Context, canaryConfig *crd.CanaryConfig) {
	ticker := time.NewTicker(canaryConfig.Spec.WeightIncrementDuration)
	quit := make(chan struct{})
	iteration := 0

	for {
		select {
		case <- (*ctx).Done():
			// this case when someone deleted their canary config in the middle of it being processed
			log.Printf("Cancel Func called for canary config : %s", canaryConfig.Metadata.Name)
			ticker.Stop()
			return

		case <- ticker.C:
			iteration += 1
			// every weightIncrementDuration, check if failureThreshold has reached.
			// if yes, rollback.
			// else, increment the weight of funcN and decrement funcN-1 by `weightIncrement`

			log.Printf("Processing canary config : %s, iteration : %d", canaryConfig.Metadata.Name, iteration)
			canaryCfgMgr.IncrementWeightOrRollback(canaryConfig, quit, &iteration)


		case <- quit:
			// we're done processing this canary config either because the new function receives 100% of the traffic
			// or we rolled back to send all 100% traffic to old function
			log.Printf("Quit processing canaryConfig : %s", canaryConfig.Metadata.Name)
			ticker.Stop()
			return
		}
	}
}

func(canaryCfgMgr *canaryConfigMgr) IncrementWeightOrRollback(canaryConfig *crd.CanaryConfig, quit chan struct{}, iteration *int) {
	// get the http trigger object associated with this canary config
	triggerObj, err := canaryCfgMgr.getHttpTriggerObject(canaryConfig.Spec.Trigger, canaryConfig.Metadata.Namespace)
	if err != nil {
		// just silently ignore. wait for next window to increment weight
		log.Printf("Error fetching http trigger object, err : %v", err)
		return
	}

	if *iteration != 1 {
		//log.Printf("value of getLatestValueFromMetrics : %v, iteration : %v", getLatestValueFromMetrics, *iteration)
		failurePercent, err := canaryCfgMgr.promClient.GetFunctionFailurePercentage(triggerObj.Spec.RelativeURL, triggerObj.Spec.Method,
			canaryConfig.Spec.FunctionN, canaryConfig.Metadata.Namespace, canaryConfig.Spec.WeightIncrementDuration)

		if err != nil {
			// silently ignore. wait for next window to increment weight
			log.Printf("Error calculating failure percentage, err : %v", err)
			return
		}

		log.Printf("Failure percentage calculated : %v for canaryConfig %s", failurePercent, canaryConfig.Metadata.Name)
		if failurePercent == -1 {
			// this means there were no requests triggered to this url during this window. return here and check back
			// during next iteration
			log.Printf("Total requests received for url : %v is 0", triggerObj.Spec.RelativeURL)
			return
		}

		// TODO : The right thing to do here is not pass the trigger object. because we might run into `StatusConflict` issue
		// change it to do a get and then update inside rollback
		if int(failurePercent) > canaryConfig.Spec.FailureThreshold {
			log.Printf("Failure percent %v crossed the threshold %v, so rolling back", failurePercent, canaryConfig.Spec.FailureThreshold)
			canaryCfgMgr.rollback(canaryConfig, triggerObj)
			close(quit)
			return
		}
	}

	doneProcessingCanaryConfig, err := canaryCfgMgr.incrementWeights(canaryConfig, triggerObj)
	if err != nil {
		// just log the error and hope that next iteration will succeed
		log.Printf("Error incrementing weights for triggerObj : %v, err : %v", triggerObj.Metadata.Name, err)
		return
	}

	if doneProcessingCanaryConfig {
		log.Printf("We're done processing canary config : %s. The new function is receiving all the traffic", canaryConfig.Metadata.Name)
		close(quit)
		return
	}
	//log.Printf("Finished this iteration of incrementWeightOrRollback for canaryConfig : %s", canaryConfig.Metadata.Name)
}

func(canaryCfgMgr *canaryConfigMgr) getHttpTriggerObject(triggerName, triggerNamespace string) (*crd.HTTPTrigger, error) {
	// TODO : Add retries
	return canaryCfgMgr.fissionClient.HTTPTriggers(triggerNamespace).Get(triggerName)
}

func(canaryCfgMgr *canaryConfigMgr) rollback(canaryConfig *crd.CanaryConfig, trigger *crd.HTTPTrigger) error {
	// TODO : Add retries
	functionWeights := trigger.Spec.FunctionReference.FunctionWeights
	functionWeights[canaryConfig.Spec.FunctionN] = 0
	functionWeights[canaryConfig.Spec.FunctionNminus1] = 100

	trigger.Spec.FunctionReference.FunctionWeights = functionWeights

	_, err := canaryCfgMgr.fissionClient.HTTPTriggers(trigger.Metadata.Namespace).Update(trigger)
	if err != nil {
		log.Printf("Error updating http trigger object, err : %v", err)
		return err
	}
	//log.Printf("Successfully updated http trigger for rollback : %v", trigger.Metadata.Name)
	return nil
}

func(canaryCfgMgr *canaryConfigMgr) incrementWeights(canaryConfig *crd.CanaryConfig, trigger *crd.HTTPTrigger) (bool, error) {
	// TODO : Add retries
	doneProcessingCanaryConfig := false

	functionWeights := trigger.Spec.FunctionReference.FunctionWeights
	if functionWeights[canaryConfig.Spec.FunctionN] + canaryConfig.Spec.WeightIncrement >= 100 {
		doneProcessingCanaryConfig = true
		functionWeights[canaryConfig.Spec.FunctionN] = 100
		functionWeights[canaryConfig.Spec.FunctionNminus1] = 0
	} else {
		functionWeights[canaryConfig.Spec.FunctionN] += canaryConfig.Spec.WeightIncrement
		if functionWeights[canaryConfig.Spec.FunctionNminus1] - canaryConfig.Spec.WeightIncrement < 0 {
			functionWeights[canaryConfig.Spec.FunctionNminus1] = 0
		} else {
			functionWeights[canaryConfig.Spec.FunctionNminus1] -= canaryConfig.Spec.WeightIncrement
		}
	}

	log.Printf("Final incremented functionWeights : %v", functionWeights)
	trigger.Spec.FunctionReference.FunctionWeights = functionWeights

	_, err := canaryCfgMgr.fissionClient.HTTPTriggers(trigger.Metadata.Namespace).Update(trigger)
	if err != nil {
		log.Printf("Error updating http trigger object, err : %v", err)
		return doneProcessingCanaryConfig, err
	}

	//log.Printf("Successfully updated http trigger obj for incrementWeights : %s", trigger.Metadata.Name)
	return doneProcessingCanaryConfig, nil
}

func(canaryCfgMgr *canaryConfigMgr) reSyncCanaryConfigs() {
	for _, canaryConfig := range canaryCfgMgr.canaryConfigStore.List() {
		cancelFunc, err := canaryCfgMgr.canaryCfgCancelFuncMap.lookup(&canaryConfig.(*crd.CanaryConfig).Metadata)
		if err != nil || cancelFunc == nil {
			// new canaryConfig detected, add it to our cache and start processing it
			go canaryCfgMgr.addCanaryConfig(canaryConfig.(*crd.CanaryConfig))
		}
	}
}

func(canaryCfgMgr *canaryConfigMgr) deleteCanaryConfig(canaryConfig *crd.CanaryConfig) {
	log.Printf("Delete event received for canary config : %v, %v, %v", canaryConfig.Metadata.Name, canaryConfig.Metadata.Namespace, canaryConfig.Metadata.ResourceVersion)
	cancelFunc, err := canaryCfgMgr.canaryCfgCancelFuncMap.lookup(&canaryConfig.Metadata)
	if err != nil {
		log.Printf("Something's wrong, lookup of canaryConfig failed, err : %v", err)
		return
	}
	// when this is called, the ctx.Done returns inside processCanaryConfig function and processing gets stopped
	(*cancelFunc)()
}


func(canaryCfgMgr *canaryConfigMgr) updateCanaryConfig(oldCanaryConfig *crd.CanaryConfig, newCanaryConfig *crd.CanaryConfig) {
	// before removing the object from cache, we need to get it's cancel func and cancel it
	canaryCfgMgr.deleteCanaryConfig(oldCanaryConfig)

	err := canaryCfgMgr.canaryCfgCancelFuncMap.remove(&oldCanaryConfig.Metadata)
	if err != nil {
		log.Printf("Something's wrong, error removing canary config: %s from map, err : %v", oldCanaryConfig.Metadata.Name, err)
		return
	}
	canaryCfgMgr.addCanaryConfig(newCanaryConfig)
}
