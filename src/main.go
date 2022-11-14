package main

import (
	"encoding/json"
	"strings"

	"github.com/Shopify/sarama"
)

func main() {
	// create an Ansible module
	module := NewAnsibleModule("kafka_topic")

	// parse the command line arguments
	module.ParseArguments()

	// define the module arguments
	module.AddArgument("zookeeper", "", true)
	module.AddArgument("topic", "", true)
	module.AddArgument("partitions", "1", false)
	module.AddArgument("replication_factor", "1", false)
	module.AddArgument("config", "", false)
	module.AddArgument("state", "present", false)

	// parse the module arguments
	module.ParseArguments()

	// extract the module arguments
	zookeeper := module.GetArgument("zookeeper")
	topic := module.GetArgument("topic")
	partitions := module.GetArgument("partitions")
	replicationFactor := module.GetArgument("replication_factor")
	config := module.GetArgument("config")
	state := module.GetArgument("state")

	// parse the config
	topicConfig := make(map[string]string)
	if config != "" {
		err := json.Unmarshal([]byte(config), &topicConfig)
		if err != nil {
			module.Fail("failed to parse config: %v", err)
		}
	}

	// create a new Kafka client
	client, err := sarama.NewClient(strings.Split(zookeeper, ","), nil)
	if err != nil {
		module.Fail("failed to create Kafka client: %v", err)
	}
	defer client.Close()

	// check if the topic exists
	exists, err := client.TopicExists(topic)
	if err != nil {
		module.Fail("failed to check if topic exists: %v", err)
	}

	// check if the topic needs to be created
	if state == "present" && !exists {
		// create the topic
		err = client.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions:     int32(module.ParseInt(partitions)),
			ReplicationFactor: int16(module.ParseInt(replicationFactor)),
			ConfigEntries:     topicConfig,
		}, false)
		if err != nil {
			module.Fail("failed to create topic: %v", err)
		}

		// return the result
		module.ExitJson("created topic", map[string]interface{}{
			"topic": topic,
		})
	}

	// check if the topic needs to be deleted
	if state == "absent" && exists {
		// delete the topic
		err = client.DeleteTopic(topic)
		if err != nil {
			module.Fail("failed to delete topic: %v", err)
		}

		// return the result
		module.ExitJson("deleted topic", map[string]interface{}{
			"topic": topic,
		})
	}

	// return the result
	module.ExitJson("topic is in the desired state", map[string]interface{}{
		"topic": topic,
	})
}
