package com.example.justin.kafka.tutorial1;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class ConsumerRunnable implements Runnable {

    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;

    public ConsumerRunnable(String bootstrapServers,
                            String groupId,
                            String topic,
                            CountDownLatch latch) {
        this.latch = latch;

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // "earliest"/"latest"/"none"

        // create consumer
        consumer = new KafkaConsumer<String, String>(properties);

        // subscribe consumer to our topics(s)
        consumer.subscribe(Collections.singleton(topic));

    }


    @Override
    public void run() {
        // poll for new data
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("key: " + record.key() + ", value: " + record.value());
                    log.info("partition: " + record.partition() + ", offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Received shutdown signal");
        } finally {
            consumer.close();
            // tell our main code we are done with the consumer
            latch.countDown();
        }
    }

    public void shutdown() {
        // the wakeup() method is a special method to interrupt consumer.poll()
        // it will throw the exception WakeUpException
        consumer.wakeup();

    }
}
