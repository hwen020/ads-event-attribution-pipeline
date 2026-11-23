package portfolio.ads;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Validator;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Configuration
class KafkaConfiguration {
    static final String EVENTS = "ads.events.v1";
    static final String DLQ = "ads.events.v1.dlt";

    @Bean
    NewTopic events(@Value("${ads.partitions}") int partitions) {
        return TopicBuilder.name(EVENTS).partitions(partitions).replicas(1).build();
    }

    @Bean
    NewTopic deadLetters(@Value("${ads.partitions}") int partitions) {
        return TopicBuilder.name(DLQ).partitions(partitions).replicas(1).build();
    }

    @Bean
    DefaultErrorHandler consumerErrors() {
        return new DefaultErrorHandler(new FixedBackOff(1000, FixedBackOff.UNLIMITED_ATTEMPTS));
    }
}

@Component
class Transport {
    record Envelope(AdsEvent event, Instant receivedAt) {}
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper json;
    private final Validator validator;
    private final EventStore store;
    private final MeterRegistry meters;

    Transport(KafkaTemplate<String, String> kafka, ObjectMapper json, Validator validator,
              EventStore store, MeterRegistry meters) {
        this.kafka = kafka;
        this.json = json;
        this.validator = validator;
        this.store = store;
        this.meters = meters;
    }

    void publish(List<AdsEvent> events, String correlation) throws Exception {
        var acks = new java.util.ArrayList<CompletableFuture<?>>();
        for (var event : events) {
            var headers = new RecordHeaders();
            headers.add("correlation-id", correlation.getBytes(StandardCharsets.UTF_8));
            acks.add(kafka.send(new ProducerRecord<>(KafkaConfiguration.EVENTS, null,
                event.userId().toString(), json.writeValueAsString(new Envelope(event, event.receivedAt())), headers))
                .thenAccept(result -> meters.counter("ads.ingested").increment()));
        }
        CompletableFuture.allOf(acks.toArray(CompletableFuture[]::new)).get(14, TimeUnit.SECONDS);
    }

    @KafkaListener(topics = KafkaConfiguration.EVENTS, autoStartup = "${ads.worker}")
    void consume(ConsumerRecord<String, String> record, Acknowledgment ack) throws Exception {
        var header = record.headers().lastHeader("correlation-id");
        String correlation = header == null ? "kafka-" + record.partition() + "-" + record.offset()
            : new String(header.value(), StandardCharsets.UTF_8);
        var sample = io.micrometer.core.instrument.Timer.start(meters);
        try (var ignored = MDC.putCloseable("correlation_id", correlation)) {
            AdsEvent event;
            try {
                var envelope = json.readValue(record.value(), Envelope.class);
                if (envelope == null || envelope.event() == null || envelope.receivedAt() == null
                        || !validator.validate(envelope.event()).isEmpty()
                        || !envelope.event().userId().toString().equals(record.key())
                        || !EventApi.validTime(envelope.event().eventTime(), envelope.receivedAt())) {
                    throw new IllegalArgumentException("Invalid event envelope");
                }
                event = envelope.event().received(envelope.receivedAt());
            } catch (com.fasterxml.jackson.core.JsonProcessingException | IllegalArgumentException invalid) {
                record.headers().add("failure", "invalid-envelope".getBytes(StandardCharsets.UTF_8));
                kafka.send(new ProducerRecord<>(KafkaConfiguration.DLQ, record.partition(),
                    record.key(), record.value(), record.headers())).get(14, TimeUnit.SECONDS);
                meters.counter("ads.dlq").increment();
                LoggerFactory.getLogger(Transport.class).warn("Quarantined an invalid event");
                ack.acknowledge();
                return;
            }

            boolean inserted = store.apply(event, record.value());
            meters.counter(inserted ? "ads.processed" : "ads.deduplicated").increment();
            ack.acknowledge();
        } catch (Exception failure) {
            meters.counter("ads.processing.errors").increment();
            throw failure;
        } finally {
            sample.stop(meters.timer("ads.processing"));
        }
    }
}
