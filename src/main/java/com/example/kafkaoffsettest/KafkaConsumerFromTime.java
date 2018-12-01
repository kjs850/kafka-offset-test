package com.example.kafkaoffsettest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.DateTime;

import java.time.Instant;
import java.util.*;

import static com.example.kafkaoffsettest.KafkaConsumerUtil.TOPIC;
import static com.example.kafkaoffsettest.KafkaConsumerUtil.createConsumer;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.MINUTES;

public class KafkaConsumerFromTime {

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = createConsumer();
        consumer.subscribe(Arrays.asList(TOPIC));

        boolean flag = true;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            if (flag) {
                Set<TopicPartition> assignments = consumer.assignment();
                Map<TopicPartition, Long> query = new HashMap<>();
                for (TopicPartition topicPartition : assignments) {
                    query.put(
                            topicPartition,
                            Instant.now().minus(1, DAYS).toEpochMilli());
                }

                Map<TopicPartition, OffsetAndTimestamp> result = consumer.offsetsForTimes(query);

                result.entrySet()
                        .stream()
                        .forEach(entry ->
                                consumer.seek(
                                        entry.getKey(),
                                        Optional.ofNullable(entry.getValue())
                                                .map(OffsetAndTimestamp::offset)
                                                .orElse(new Long(0))));

                flag = false;
            }


            for (ConsumerRecord<String, String> record : records) {
                long timestamp = record.timestamp();
                DateTime dateTime = new DateTime(timestamp);
                System.out.printf("dateTime = %s, offset = %d, key = %s, value = %s%n", dateTime, record.offset(), record.key(), record.value());
            }
        }


    }
}
