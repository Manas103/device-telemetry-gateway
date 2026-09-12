package kafkaclient

import (
	"fmt"

	"github.com/segmentio/kafka-go"
)

// EnsureTopic creates a topic if it does not already exist. Shared by
// cmd/deliverygateway, cmd/deliveryburst and other delivery-stats commands;
// the storefront/segment extension's cmd/segmentbench has its own
// unexported copy of this same logic, not reused from here, to avoid making
// a change meant for one extension's command silently change another's.
func EnsureTopic(broker, topic string, partitions int) error {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		return err
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	ctrlConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return err
	}
	defer ctrlConn.Close()
	return ctrlConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	})
}
