package com.github.shintodev.kafkaexamples.demo1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ProducerWithKeys {
    private static final Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class);

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        String bootstrapServers = "localhost:29092";

        Properties properties = new Properties();
        // bootstrap.servers
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // which serializer is needed to convert the value to bytes
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            String topic = "my-topic";
            String message = "hello world " + i;
            String key = "key " + i;

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, message);
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
            }).get(); // to make it synchronous. don't do it on production.
        }

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
