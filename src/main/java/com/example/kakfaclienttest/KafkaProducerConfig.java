package com.example.kakfaclienttest;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * @author huangchen
 * @date 2020-02-10 14:24
 */
@Configuration
@ConditionalOnProperty(name = "mode", havingValue = "producer")
public class KafkaProducerConfig {
    private Logger logger = LoggerFactory.getLogger(KafkaProducerConfig.class);

    @Value("${broker}")
    private String broker;

    @Value("${topic}")
    private String topic;

    @Value("${numRecords}")
    private long numRecords;

    @Value("${messageSize}")
    private int messageSize;

    @Value("${producerCount}")
    private int producerCount;

    @Value("${throughput}")
    private int throughput;

    @Value("${printInterval}")
    private int printInterval = 5000;

    //--broker=xxxx:9092 --topic=test3 --numRecords=5000 --messageSize=400 --producerCount=10 --throughput=100 --printInterval=10000

    @Bean
    public List<KafkaProducer> listens() {
        List<KafkaProducer> producers = Lists.newArrayListWithCapacity(producerCount);


        for (int count = 0; count < producerCount; count++) {
            Properties properties = new Properties();
            properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, broker);
            properties.setProperty(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, "60000");
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization" +
                    ".ByteArraySerializer");
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization" +
                    ".ByteArraySerializer");
            KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties);



            new Thread(() -> {
                byte[] payload = new byte[0];
                Random random = new Random(0);
                if (messageSize > 0) {
                    payload = new byte[messageSize];
                    for (int i = 0; i < payload.length; ++i) {
                        payload[i] = (byte) (random.nextInt(26) + 65);
                    }
                }

                long startMs = System.currentTimeMillis();
                Stats stats = new Stats(numRecords, printInterval);
                ThroughputThrottler throttler = new ThroughputThrottler(throughput, startMs);


                ProducerRecord<byte[], byte[]> record;
                for (long i = 0; numRecords <= 0 || i < numRecords; i++) {
                    record = new ProducerRecord<>(topic, payload);
                    long sendStartMs = System.currentTimeMillis();
                    Callback cb = stats.nextCompletion(sendStartMs, payload.length, stats);
                    producer.send(record, cb);

                    if (throttler.shouldThrottle(i, sendStartMs)) {
                        throttler.throttle();
                    }
                }
            }).start();
            producers.add(producer);
        }

        return producers;
    }

    private static volatile int StatsId = 0;

    private static class Stats {
        private int id;
        private long start;
        private long windowStart;
        private int[] latencies;
        private int sampling;
        private int iteration;
        private int index;
        private long count;
        private long bytes;
        private int maxLatency;
        private long totalLatency;
        private long windowCount;
        private int windowMaxLatency;
        private long windowTotalLatency;
        private long windowBytes;
        private long reportingInterval;

        public Stats(long numRecords, int reportingInterval) {
            this.id = StatsId++;
            this.start = System.currentTimeMillis();
            this.windowStart = System.currentTimeMillis();
            this.index = 0;
            this.iteration = 0;
            this.sampling = (int) (numRecords / Math.min(numRecords, 500000));
            this.latencies = new int[(int) (numRecords / this.sampling) + 1];
            this.index = 0;
            this.maxLatency = 0;
            this.totalLatency = 0;
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.totalLatency = 0;
            this.reportingInterval = reportingInterval;
        }

        public void record(int iter, int latency, int bytes, long time) {
            this.count++;
            this.bytes += bytes;
            this.totalLatency += latency;
            this.maxLatency = Math.max(this.maxLatency, latency);
            this.windowCount++;
            this.windowBytes += bytes;
            this.windowTotalLatency += latency;
            this.windowMaxLatency = Math.max(windowMaxLatency, latency);
            if (iter % this.sampling == 0) {
                this.latencies[index] = latency;
                this.index++;
            }
            /* maybe report the recent perf */
            if (time - windowStart >= reportingInterval) {
                printWindow();
                newWindow();
            }
        }

        public Callback nextCompletion(long start, int bytes, Stats stats) {
            Callback cb = new PerfCallback(this.iteration, start, bytes, stats);
            this.iteration++;
            return cb;
        }

        public void printWindow() {
            long ellapsed = System.currentTimeMillis() - windowStart;
            double recsPerSec = 1000.0 * windowCount / (double) ellapsed;
            double mbPerSec = 1000.0 * this.windowBytes / (double) ellapsed / (1024.0 * 1024.0);
            System.out.printf("%d, %d records sent, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %.1f max latency.\n",
                    id,
                    windowCount,
                    recsPerSec,
                    mbPerSec,
                    windowTotalLatency / (double) windowCount,
                    (double) windowMaxLatency);
        }

        public void newWindow() {
            this.windowStart = System.currentTimeMillis();
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
        }

        public void printTotal() {
            long elapsed = System.currentTimeMillis() - start;
            double recsPerSec = 1000.0 * count / (double) elapsed;
            double mbPerSec = 1000.0 * this.bytes / (double) elapsed / (1024.0 * 1024.0);
            int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
            System.out.printf("%d, %d records sent, %f records/sec (%.2f MB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.\n",
                    id,
                    count,
                    recsPerSec,
                    mbPerSec,
                    totalLatency / (double) count,
                    (double) maxLatency,
                    percs[0],
                    percs[1],
                    percs[2],
                    percs[3]);
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

    private static final class PerfCallback implements Callback {
        private final long start;
        private final int iteration;
        private final int bytes;
        private final Stats stats;

        public PerfCallback(int iter, long start, int bytes, Stats stats) {
            this.start = start;
            this.stats = stats;
            this.iteration = iter;
            this.bytes = bytes;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long now = System.currentTimeMillis();
            int latency = (int) (now - start);
            this.stats.record(iteration, latency, bytes, now);
            if (exception != null) {
                exception.printStackTrace();
            }
        }
    }
}
