package com.example.kakfaclienttest;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.*;

/**
 * @author huangchen
 * @date 2020-02-10 14:24
 */
@Configuration
@ConditionalOnProperty(name = "mode", havingValue = "consumer")
public class KafkaConsumerConfig {
    private Logger logger = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Value("${broker}")
    private String broker;

    @Value("${topic}")
    private String topic;

    @Value("${numRecords:-1}")
    private long numRecords;

    @Value("${consumerCount:1}")
    private int consumerCount;

    @Value("${max.poll.records:1000}")
    private int maxPollRecords = 1000;

    @Value("${printInterval:5000}")
    private int printInterval = 5000;

    @Value("${sleep.time:100}")
    private int sleepTime = 100;

    //--broker=xxxx:9092 --topic=test3 --numRecords=500000  --consumerCount=1 --printInterval=10000

    @Bean
    public List<KafkaConsumer> consumers() {
        List<KafkaConsumer> consumers = Lists.newArrayListWithCapacity(consumerCount);

        String nodeId = UUID.randomUUID().toString().hashCode() + "";

        for (int count = 0; count < consumerCount; count++) {
            String groupId = nodeId + "_" + count;
            Properties properties = new Properties();
            properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, broker);
            properties.setProperty(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, "60000");
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            properties.setProperty("enable.auto.commit", "true");
            properties.setProperty("auto.commit.interval.ms", "1000");
            properties.setProperty("auto.offset.reset", "earliest");
            properties.setProperty("max.poll.records", maxPollRecords + "");
            properties.setProperty("group.id", groupId);

            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Lists.newArrayList(topic));

            new Thread(() -> {
                Stats stats = new Stats(groupId, printInterval);

                while (true) {
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(100);

                    stats.record(records);

                    try {
                        //模拟业务处理
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    if (numRecords > 0 && stats.count > numRecords) {
                        System.out.printf("%s, end\n", groupId);
                        stats.print();
                        consumer.close();
                        break;
                    }
                }
            }).start();
            consumers.add(consumer);
        }

        return consumers;
    }

    private static class Stats {
        private String id;
        private long startTime;
        private long bytes;
        private long count;
        private long reportingInterval;
        private long windowStart;

        public Stats(String id, int reportingInterval) {
            this.id = id;
            this.reportingInterval = reportingInterval;
            this.windowStart = System.currentTimeMillis();
            this.startTime = System.currentTimeMillis();
        }

        public void record(ConsumerRecords<byte[], byte[]> records) {
            if (records.count() == 0) {
                return;
            }

            this.count += records.count();
            this.bytes += records.count() * records.iterator().next().value().length;

            /* maybe report the recent perf */
            if (System.currentTimeMillis() - windowStart >= reportingInterval) {
                print();
                newWindow();
            }
        }


        public void print() {
            long ellapsed = System.currentTimeMillis() - startTime;
            double recsPerSec = this.count * 1000 / ellapsed;
            double mbPerSec = 1000.0 * this.bytes / (double) ellapsed / (1024.0 * 1024.0);
            System.out.printf("%s, %d total rcv, %.1f records/sec (%.2f MB/sec)\n",
                    id,
                    count,
                    recsPerSec,
                    mbPerSec);
        }

        public void newWindow() {
            this.windowStart = System.currentTimeMillis();
        }

        private static int[] percentiles(int[] latencies, int count, double... percentiles) {
            int size = Math.min(count, latencies.length);
            Arrays.sort(latencies, 0, size);
            int[] values = new int[percentiles.length];
            for (int i = 0; i < percentiles.length; i++) {
                int index = (int) (percentiles[i] * size);
                values[i] = latencies[index];
            }
            return values;
        }
    }
}
