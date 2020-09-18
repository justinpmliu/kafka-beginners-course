package com.example.justin.kafka.tutorial1;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CountDownLatch;

@Slf4j
public class ConsumerDemoWithThread {

    public ConsumerDemoWithThread() {}

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    public void run() {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-second-application";
        String topic = "first_topic";

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // create the consumer runnable
        log.info("Creating the consumer thread");
        ConsumerRunnable consumerRunnable = new ConsumerRunnable(bootstrapServers, groupId, topic, latch);

        // start the thread
        Thread consumerThread = new Thread(consumerRunnable);
        consumerThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            log.info("Caught shutdown hook");
            consumerRunnable.shutdown();

            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("Application has exited");

        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Application got interrupted", e);
        } finally {
            log.info("Application is closing");
        }

    }


}
