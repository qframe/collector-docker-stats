package qcollector_docker_stats

import (
	"strings"
	"fmt"
	"log"
	"github.com/fsouza/go-dockerclient"

	"github.com/qframe/types/qchannel"
	"github.com/pkg/errors"
	"github.com/qframe/types/metrics"
)

// struct to keep info and channels to goroutine
// -> get heartbeats so that we know it's still alive
// -> allow for gracefully shutdown of the supervisor
type ContainerSupervisor struct {
	CntID 	string 			 // ContainerID
	CntName string			 // sanatized name of container
	Container docker.Container
	Com 	chan interface{} // Channel to communicate with goroutine
	cli 	*docker.Client
	qChan 	qtypes_qchannel.QChan
}

func SplitLabels(labels []string) map[string]string {
	res := map[string]string{}
	for _, label := range labels {
		tupel := strings.Split(label, "=")
		res[tupel[0]] = tupel[1]
	}
	return res
}

func (cs ContainerSupervisor) Run() {
	log.Printf("[II] Start listener for: '%s' [%s]", cs.CntName, cs.CntID)
	// TODO: That is not realy straight forward...
	filter := map[string][]string{
		"id": []string{cs.CntID},
	}
	df := docker.ListContainersOptions{
		Filters: filter,
	}
	info, _ := cs.cli.Info()
	engineLabels := SplitLabels(info.Labels)
	cnt, _ := cs.cli.ListContainers(df)
	if len(cnt) != 1 {
		log.Printf("[EE] Could not found excatly one container with id '%s'", cs.CntID)
		return
	}
	errChannel := make(chan error, 1)
	statsChannel := make(chan *docker.Stats)

	opts := docker.StatsOptions{
		ID:     cs.CntID,
		Stats:  statsChannel,
		Stream: true,
	}

	go func() {
		errChannel <- cs.cli.Stats(opts)
	}()

	for {
		select {
		case msg := <-cs.Com:
			switch msg {
			case "died":
				log.Printf("[DD] Container [%s]->'%s' died -> BYE!", cs.CntID, cs.CntName)
				return
			default:
				log.Printf("[DD] Container [%s]->'%s' got message from cs.Com: %v\n", cs.CntID, cs.CntName, msg)
			}
		case stats, ok := <-statsChannel:
			if !ok {
				err := errors.New(fmt.Sprintf("Bad response getting stats for container: %s", cs.CntID))
				log.Println(err.Error())
				return
			}
			qs := qtypes_metrics.NewContainerStats("docker-stats", stats, cnt[0])
			for k, v := range engineLabels {
				qs.Container.Labels[k] = v
			}
			cs.qChan.Data.Send(qs)
		}
	}
}