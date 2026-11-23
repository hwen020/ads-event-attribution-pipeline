package portfolio.ads;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.scheduling.annotation.Scheduled;
import jakarta.annotation.PreDestroy;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Component
class LagMonitor {
    private final Admin admin;
    private final String group;
    private final AtomicLong lag = new AtomicLong(-1);

    LagMonitor(@Value("${spring.kafka.bootstrap-servers}") String bootstrap,
               @Value("${spring.kafka.consumer.group-id}") String group, MeterRegistry meters) {
        this.group = group;
        admin = Admin.create(Map.of("bootstrap.servers", bootstrap, "default.api.timeout.ms", 2500, "request.timeout.ms", 1500));
        meters.gauge("ads.consumer.lag", lag);
    }

    @Scheduled(fixedDelay = 10000)
    void update() {
        try {
            String topic = KafkaConfiguration.EVENTS;
            var description = admin.describeTopics(List.of(topic)).allTopicNames().get(3, TimeUnit.SECONDS);
            var latestQuery = new HashMap<TopicPartition, OffsetSpec>();
            var earliestQuery = new HashMap<TopicPartition, OffsetSpec>();
            for (var partition : description.get(topic).partitions()) {
                var key = new TopicPartition(topic, partition.partition());
                latestQuery.put(key, OffsetSpec.latest());
                earliestQuery.put(key, OffsetSpec.earliest());
            }
            var latest = admin.listOffsets(latestQuery).all().get(3, TimeUnit.SECONDS);
            var earliest = admin.listOffsets(earliestQuery).all().get(3, TimeUnit.SECONDS);
            var committed = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                .get(3, TimeUnit.SECONDS);
            long total = 0;
            for (var entry : latest.entrySet()) {
                var previous = committed.get(entry.getKey());
                long start = earliest.get(entry.getKey()).offset();
                long offset = previous == null ? start : Math.max(start, previous.offset());
                total += Math.max(0, entry.getValue().offset() - offset);
            }
            lag.set(total);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            lag.set(-1);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException unavailable) {
            lag.set(-1);
        }
    }

    @PreDestroy
    void close() {
        admin.close(java.time.Duration.ofSeconds(1));
    }
}
