package com.github.shintodev.kafkaexamples.demo1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ProducerWithCallback {
    private static final Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);

    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = "localhost:29092";

        Properties properties = new Properties();
        // bootstrap.servers
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // which serializer is needed to convert the value to bytes
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("my-topic", "hello world " + i);
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Error while producing", exception);
                    return;
                }
                // the record was successfully sent.
                logger.info("received new metadata. \n" +
                        "Topic: " + metadata.topic() + "\n" +
                        "Partition: " + metadata.partition() + "\n" +
                        "Offset: " + metadata.offset()
                );
            });
        }

        kafkaProducer.flush();
        TimeUnit.SECONDS.sleep(2);
        kafkaProducer.close();
    }
}
