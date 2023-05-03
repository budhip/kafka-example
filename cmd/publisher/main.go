package main

import (
	"crypto/tls"
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/budhip/messaging/kafka"
	"github.com/sirupsen/logrus"

	tls2 "github.com/budhip/common/tls"
)

func getKafkaConfig(tlsConfig *tls.Config) *kafka.Config {
	config := sarama.NewConfig()
	if tlsConfig != nil {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
	}
	logrus.Println("y 1")
	return &kafka.Config{Config: config, AutoAck: true}
}

func getKafkaClient(kafkaAddresses []string, tlsConfig *tls.Config) (*kafka.Client, error) {
	logrus.Println("z 1")
	config := getKafkaConfig(tlsConfig)
	logrus.Println("z 2")
	return kafka.NewClient(kafkaAddresses, config)
}

func main() {
	// Setup Logging
	customFormatter := new(logrus.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.FullTimestamp = true
	logrus.SetFormatter(customFormatter)

	kafkaAddr := []string{"localhost:9095", "localhost:9096", "localhost:9097"}
	//kafkaAddr := []string{"localhost:9095"}
	var kafkaCa, kafkaCert, kafkaKey []byte
	logrus.Println("A")

	tlsConfig := tls2.WithCertificate(kafkaCa, kafkaCert, kafkaKey, false)
	logrus.Println("B")

	msgClient, msgErr := getKafkaClient(kafkaAddr, tlsConfig)
	logrus.Println("ssdfsdf")
	if msgErr != nil {
		logrus.Println("C")
		logrus.Error("message error: ", msgErr)
	}
	logrus.Println("D")
	logrus.Println("publisher set to kafka cluster", kafkaAddr)


	logrus.Println("msgClient: ", msgClient)

	msg := map[string]interface{}{
		"message": "halo",
		"from": "your heart",
	}

	msgByte, errMsgByte := json.Marshal(msg)
	if errMsgByte != nil {
		logrus.Println("error byting message: ", errMsgByte)
	}


	errPublishKafka := msgClient.Publish("test2", msgByte)
	if errPublishKafka != nil {
		logrus.Error("err sending message ke kafka topic: ", errPublishKafka)
	}
}
