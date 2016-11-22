package logstash

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"strings"

	"github.com/fsouza/go-dockerclient"
	"github.com/gliderlabs/logspout/router"
)

func init() {
	router.AdapterFactories.Register(NewLogstashAdapter, "logstash")
}

// LogstashAdapter is an adapter that streams UDP JSON to Logstash.
type LogstashAdapter struct {
	conn          net.Conn
	route         *router.Route
	containerTags map[string][]string
}

// NewLogstashAdapter creates a LogstashAdapter with UDP as the default transport.
func NewLogstashAdapter(route *router.Route) (router.LogAdapter, error) {
	transport, found := router.AdapterTransports.Lookup(route.AdapterTransport("udp"))
	if !found {
		return nil, errors.New("unable to find adapter: " + route.Adapter)
	}

	conn, err := transport.Dial(route.Address, route.Options)
	if err != nil {
		return nil, err
	}

	return &LogstashAdapter{
		route:         route,
		conn:          conn,
		containerTags: make(map[string][]string),
	}, nil
}

// Get container tags configured with the environment variable LOGSTASH_TAGS
func GetContainerTags(c *docker.Container, a *LogstashAdapter) []string {
	if tags, ok := a.containerTags[c.ID]; ok {
		return tags
	}

	var tags = []string{}
	for _, e := range c.Config.Env {
		if strings.HasPrefix(e, "LOGSTASH_TAGS=") {
			tags = strings.Split(strings.TrimPrefix(e, "LOGSTASH_TAGS="), ",")
			break
		}
	}

	a.containerTags[c.ID] = tags
	return tags
}

// func GetMarathonData(c *docker.Container, a *LogstashAdapter) map[string]string {
//func GetMarathonData(c *docker.Container, a *LogstashAdapter) MarathonData {
//func (c *docker.Container, a *LogstashAdapter, m *MarathonData) SetMarathonData() {
func GetMarathonData(c *docker.Container, a *LogstashAdapter) MarathonData {

	// type MarathonData struct {
	// 	Version  string
	// 	Resource map[string]string
	// 	ID       string
	// 	Label    map[string]string
	//  Image    string
	// }

	// marathondata := map[string]string{}
	// var marathondata map[string]string
	m := MarathonData{}

	/*

		"MARATHON_APP_VERSION=2016-10-20T13:25:13.627Z",
		"MARATHON_APP_LABEL_ENVIRONMENT=prod",
		"MARATHON_APP_RESOURCE_CPUS=0.01",
		"MARATHON_APP_LABEL_VERSION=1.6",
		"MARATHON_APP_DOCKER_IMAGE=ops-mesos-registry.vast.com:5000/vast-flapjack-notifier:1.6",
		"MESOS_TASK_ID=flapjack-notifier.c101b8cd-a1ca-11e6-a07b-024232c1c875",
		"MARATHON_APP_RESOURCE_MEM=128.0",
		"MARATHON_APP_RESOURCE_DISK=0.0",
		"MARATHON_APP_LABELS=VERSION
		"MARATHON_APP_ID=/flapjack-notifier",
		"MESOS_SANDBOX=/mnt/mesos/sandbox",
		"MESOS_CONTAINER_NAME=mesos-04fb9b4e-ccdd-4884-b2b6-11c88c04760c-S14.9ef25b40-3d77-4dd9-b5b6-04b3bd02435b",

	*/

	for _, e := range c.Config.Env {
		if strings.HasPrefix(e, "MARATHON_APP_LABEL_") {
			kv := strings.Split(strings.TrimPrefix(e, "MARATHON_APP_LABEL_"), "=")
			// k, v := kv[0], kv[1]
			m.Label[kv[0]] = kv[1]
			// log.Println("logstash: Marathon info:", marathondata)
		} else if strings.HasPrefix(e, "MARATHON_APP_RESOURCE_CPUS=") {
			m.Resource["cpus"] = strings.TrimPrefix(e, "MARATHON_APP_RESOURCE_CPUS=")
		} else if strings.HasPrefix(e, "MARATHON_APP_RESOURCE_MEM=") {
			m.Resource["mem"] = strings.TrimPrefix(e, "MARATHON_APP_RESOURCE_MEM=")
		} else if strings.HasPrefix(e, "MARATHON_APP_RESOURCE_DISK=") {
			m.Resource["disk"] = strings.TrimPrefix(e, "MARATHON_APP_RESOURCE_DISK=")
		} else if strings.HasPrefix(e, "MARATHON_APP_ID=") {
			m.ID = strings.TrimPrefix(e, "MARATHON_APP_ID=")
		} else if strings.HasPrefix(e, "MARATHON_APP_VERSION=") {
			m.Version = strings.TrimPrefix(e, "MARATHON_APP_VERSION=")
		} else if strings.HasPrefix(e, "MARATHON_APP_DOCKER_IMAGE=") {
			m.Image = strings.TrimPrefix(e, "MARATHON_APP_DOCKER_IMAGE=")
		}
	}

	return m
}

// Stream implements the router.LogAdapter interface.
func (a *LogstashAdapter) Stream(logstream chan *router.Message) {

	for m := range logstream {

		dockerInfo := DockerInfo{
			Name:     m.Container.Name,
			ID:       m.Container.ID,
			Image:    m.Container.Config.Image,
			Hostname: m.Container.Config.Hostname,
		}

		tags := GetContainerTags(m.Container, a)
		// marathonData := GetMarathonData(m.Container, a)
		marathonData := GetMarathonData(m.Container, a)

		var js []byte
		var data map[string]interface{}

		// Parse JSON-encoded m.Data
		if err := json.Unmarshal([]byte(m.Data), &data); err != nil {
			// The message is not in JSON, make a new JSON message.
			msg := LogstashMessage{
				Message:  m.Data,
				Docker:   dockerInfo,
				Marathon: marathonData,
				Stream:   m.Source,
				Tags:     tags,
			}

			if js, err = json.Marshal(msg); err != nil {
				// Log error message and continue parsing next line, if marshalling fails
				log.Println("logstash: could not marshal JSON:", err)
				continue
			}
		} else {
			// The message is already in JSON, add the docker specific fields.
			data["docker"] = dockerInfo
			data["tags"] = tags
			data["stream"] = m.Source
			data["marathon"] = marathonData
			// Return the JSON encoding
			if js, err = json.Marshal(data); err != nil {
				// Log error message and continue parsing next line, if marshalling fails
				log.Println("logstash: could not marshal JSON:", err)
				continue
			}
		}

		// To work with tls and tcp transports via json_lines codec
		js = append(js, byte('\n'))

		if _, err := a.conn.Write(js); err != nil {
			// There is no retry option implemented yet
			log.Fatal("logstash: could not write:", err)
		}
	}
}

type DockerInfo struct {
	Name     string `json:"name"`
	ID       string `json:"id"`
	Image    string `json:"image"`
	Hostname string `json:"hostname"`
}

// LogstashMessage is a simple JSON input to Logstash.
type LogstashMessage struct {
	Message string     `json:"message"`
	Stream  string     `json:"stream"`
	Docker  DockerInfo `json:"docker"`
	// Marathon map[string]string `json:"marathon"`
	Marathon MarathonData `json:"marathon,omitempty"`
	Mesos    MesosData    `json:"mesos,omitempty"`
	Tags     []string     `json:"tags"`
}

/*

	"MARATHON_APP_VERSION=2016-10-20T13:25:13.627Z",
	"MARATHON_APP_LABEL_ENVIRONMENT=prod",
	"MARATHON_APP_RESOURCE_CPUS=0.01",
	"MARATHON_APP_LABEL_VERSION=1.6",
	"MARATHON_APP_DOCKER_IMAGE=ops-mesos-registry.vast.com:5000/vast-flapjack-notifier:1.6",
	"MESOS_TASK_ID=flapjack-notifier.c101b8cd-a1ca-11e6-a07b-024232c1c875",
	"MARATHON_APP_RESOURCE_MEM=128.0",
	"MARATHON_APP_RESOURCE_DISK=0.0",
	"MARATHON_APP_LABELS=VERSION
	"MARATHON_APP_ID=/flapjack-notifier",
	"MESOS_SANDBOX=/mnt/mesos/sandbox",
	"MESOS_CONTAINER_NAME=mesos-04fb9b4e-ccdd-4884-b2b6-11c88c04760c-S14.9ef25b40-3d77-4dd9-b5b6-04b3bd02435b",

*/

type MarathonData struct {
	Version  string
	Resource map[string]string
	ID       string
	Label    map[string]string
	Image    string
}

type MesosData struct {
	Sandbox       string
	ContainerName string
	Task          string
}
