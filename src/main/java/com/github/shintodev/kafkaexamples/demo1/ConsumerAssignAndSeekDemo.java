package com.github.shintodev.kafkaexamples.demo1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerAssignAndSeekDemo {
    static Logger logger = LoggerFactory.getLogger(ConsumerAssignAndSeekDemo.class);

    public static void main(String[] args) {
        String bootstrapServers = "localhost:29092";
        String groupId = "my-group";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earliest/latest/none

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Assign and seek are mostly used to replay data or fetch a specific message
        // Assign
        TopicPartition partitionToReadFrom = new TopicPartition("my-topic", 1);
        long offsetToReadFrom = 15L;
        consumer.assign(List.of(partitionToReadFrom));

        // Seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        int numberOfMessagesRead = 0;
        boolean keepReading = true;

        while(keepReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<String, String> record:records) {
                logger.info("key = %s, partition = %d, value=%s".formatted(record.key(), record.partition(), record.value()));
                numberOfMessagesRead++;

                if(numberOfMessagesRead == numberOfMessagesToRead) {
                    keepReading = false;
                    break;
                }
            }
        }
    }
}
