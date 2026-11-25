package portfolio.ads;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.TopicPartition;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doAnswer;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {"ads.worker=true", "spring.kafka.listener.concurrency=2"})
class PipelineIT {
    @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");
    @Container static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17");

    @DynamicPropertySource
    static void settings(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @LocalServerPort int port;
    @Autowired JdbcTemplate db;
    @Autowired javax.sql.DataSource dataSource;
    @Autowired Replay replay;
    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired ConsumerFactory<String, String> consumerFactory;
    @Autowired KafkaListenerEndpointRegistry listeners;
    @Autowired Transport transport;
    @Autowired ObjectMapper json;
    @MockitoSpyBean EventStore store;
    private final HttpClient http = HttpClient.newHttpClient();

    private static final Duration WAIT = Duration.ofSeconds(45);

    @Test
    void repairsLateArrivalAndReplaysWithoutDoubleCounting() throws Exception {
        var now = Instant.now();
        var user = UUID.randomUUID();
        var conversion = conversion(user, now.minusSeconds(10), UUID.randomUUID());
        send(conversion);
        await().atMost(WAIT).untilAsserted(() -> assertThat(result(conversion).get("touch_id")).isNull());
        var view = touch(user, now.minusSeconds(20), AdsEvent.Kind.IMPRESSION);
        send(view);
        awaitWinner(conversion, view);
        var click = touch(user, now.minusSeconds(30), AdsEvent.Kind.CLICK);
        assertThat(click.campaignId()).isNotEqualTo(view.campaignId());
        send(click);
        awaitWinner(conversion, click);
        assertThat(db.queryForObject("SELECT coalesce(sum(conversions),0) FROM campaign_metrics WHERE campaign_id=?",
            Long.class, view.campaignId())).isZero();
        assertThat(db.queryForObject("SELECT coalesce(sum(conversions),0) FROM campaign_metrics WHERE campaign_id=?",
            Long.class, click.campaignId())).isEqualTo(1L);
        assertThat(db.queryForObject("SELECT coalesce(sum(revenue),0) FROM campaign_metrics WHERE campaign_id=?",
            BigDecimal.class, view.campaignId())).isEqualByComparingTo("0");
        assertThat(db.queryForObject("SELECT coalesce(sum(revenue),0) FROM campaign_metrics WHERE campaign_id=?",
            BigDecimal.class, click.campaignId())).isEqualByComparingTo("12.50");
        var tie = touch(user, click.eventTime(), AdsEvent.Kind.CLICK);
        send(tie);
        send(tie);
        var businessDuplicate = conversion(user, conversion.eventTime(), conversion.conversionId());
        send(businessDuplicate);
        await().atMost(WAIT).untilAsserted(() -> assertThat(db.queryForObject(
            "SELECT count(*) FROM raw_events WHERE user_id=?", Long.class, user)).isEqualTo(5));
        UUID expected = click.eventId().toString().compareTo(tie.eventId().toString()) > 0
            ? click.eventId() : tie.eventId();
        assertThat(result(conversion).get("touch_id")).isEqualTo(expected);
        long version = ((Number) result(conversion).get("version")).longValue();
        UUID job = replay.create(now.minusSeconds(90), now.plusSeconds(90), null);
        await().atMost(WAIT).untilAsserted(() -> assertThat(replay.get(job).get("status")).isEqualTo("COMPLETE"));
        assertThat(replay.get(job).get("corrected")).isEqualTo(0L);
        assertThat(result(conversion).get("version")).isEqualTo(version);
        assertThat(db.queryForObject("SELECT count(*) FROM attribution_results WHERE user_id=?",
            Long.class, user)).isEqualTo(1);
        assertThat(((BigDecimal) result(conversion).get("value"))).isEqualByComparingTo("12.50");
    }

    @Test
    void retriesAfterDatabaseCommitBeforeAcknowledgingKafka() throws Exception {
        var user = UUID.randomUUID();
        var time = Instant.now();
        var conversion = conversion(user, time, UUID.randomUUID());
        listeners.stop();
        TopicPartition partition;
        long offset;
        try {
            String envelope = json.writeValueAsString(new Transport.Envelope(conversion, time));
            var position = kafka.send(KafkaConfiguration.EVENTS, user.toString(), envelope)
                .get(10, TimeUnit.SECONDS).getRecordMetadata();
            partition = new TopicPartition(position.topic(), position.partition());
            offset = position.offset();
            try (var probe = consumerFactory.createConsumer("fault-probe", "fault-probe-", "")) {
                probe.assign(List.of(partition));
                probe.seek(partition, offset);
                var records = probe.poll(Duration.ofSeconds(10)).records(partition);
                assertThat(records).hasSize(1);
                assertThatThrownBy(() -> transport.consume(records.getFirst(), () -> {
                    try (var independent = dataSource.getConnection();
                         var query = independent.prepareStatement("SELECT 1 FROM raw_events WHERE event_id=?")) {
                        query.setObject(1, conversion.eventId());
                        try (var rows = query.executeQuery()) {
                            assertThat(rows.next()).as("row committed before acknowledgment").isTrue();
                        }
                    } catch (java.sql.SQLException failure) {
                        throw new AssertionError(failure);
                    }
                    throw new IllegalStateException("Simulated crash before ack");
                })).isInstanceOf(IllegalStateException.class).hasMessage("Simulated crash before ack");
            }
        } finally {
            listeners.start();
        }

        try (var admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            await().atMost(WAIT).untilAsserted(() -> {
                var offsets = admin.listConsumerGroupOffsets("ads-attribution").partitionsToOffsetAndMetadata()
                    .get(5, TimeUnit.SECONDS);
                assertThat(offsets.get(partition)).isNotNull();
                assertThat(offsets.get(partition).offset()).isGreaterThan(offset);
            });
        }
        assertThat(db.queryForObject("SELECT count(*) FROM raw_events WHERE user_id=?", Long.class, user))
            .isEqualTo(1);
        assertThat(db.queryForObject("SELECT count(*) FROM attribution_results WHERE user_id=?", Long.class, user))
            .isEqualTo(1);
    }

    @Test
    void preservesFirstPayloadWhenAnEventIdIsReused() throws Exception {
        var time = Instant.now();
        var original = touch(UUID.randomUUID(), time, AdsEvent.Kind.CLICK).received(time);
        var conversion = conversion(UUID.randomUUID(), time, UUID.randomUUID()).received(time);
        var conflict = new AdsEvent(original.eventId(), conversion.userId(), conversion.type(),
            conversion.campaignId(), conversion.adId(), conversion.conversionId(),
            conversion.conversionValue(), conversion.currency(), conversion.eventTime(),
            conversion.receivedAt(), conversion.source());
        var originalPayload = json.writeValueAsString(new Transport.Envelope(original, time));
        var conflictingPayload = json.writeValueAsString(new Transport.Envelope(conflict, time));

        assertThat(store.apply(original, originalPayload)).isTrue();
        assertThat(store.apply(conflict, conflictingPayload)).isFalse();

        assertThat(db.queryForObject("SELECT count(*) FROM raw_events WHERE event_id=?",
            Long.class, original.eventId())).isEqualTo(1);
        assertThat(db.queryForObject("SELECT user_id FROM raw_events WHERE event_id=?",
            UUID.class, original.eventId())).isEqualTo(original.userId());
        assertThat(db.queryForObject("SELECT kind FROM raw_events WHERE event_id=?",
            String.class, original.eventId())).isEqualTo("CLICK");
        assertThat(db.queryForObject("SELECT payload FROM raw_events WHERE event_id=?",
            String.class, original.eventId())).isEqualTo(originalPayload);
        assertThat(db.queryForObject("SELECT count(*) FROM attribution_results WHERE conversion_id=?",
            Long.class, conflict.conversionId())).isZero();
    }

    @Test
    void concurrentDuplicateEventIdsCreateOnlyOneBusinessEffect() throws Exception {
        var time = Instant.now();
        var user = UUID.randomUUID();
        var eventId = UUID.randomUUID();
        var initial = conversion(user, time, UUID.randomUUID()).received(time);
        var first = new AdsEvent(eventId, initial.userId(), initial.type(), initial.campaignId(),
            initial.adId(), initial.conversionId(), initial.conversionValue(), initial.currency(),
            initial.eventTime(), initial.receivedAt(), initial.source());
        var second = new AdsEvent(eventId, user, AdsEvent.Kind.CONVERSION, null, null, UUID.randomUUID(),
            new BigDecimal("98.76"), "USD", time, time, "concurrent-duplicate");
        var firstPayload = json.writeValueAsString(new Transport.Envelope(first, time));
        var secondPayload = json.writeValueAsString(new Transport.Envelope(second, time));
        var ready = new CountDownLatch(2);
        var start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var firstWrite = executor.submit(() -> {
                ready.countDown();
                if (!start.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Start timed out");
                return store.apply(first, firstPayload);
            });
            var secondWrite = executor.submit(() -> {
                ready.countDown();
                if (!start.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Start timed out");
                return store.apply(second, secondPayload);
            });
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            int inserted = (firstWrite.get(20, TimeUnit.SECONDS) ? 1 : 0)
                + (secondWrite.get(20, TimeUnit.SECONDS) ? 1 : 0);
            assertThat(inserted).isEqualTo(1);
        } finally {
            start.countDown();
            executor.shutdownNow();
        }

        assertThat(db.queryForObject("SELECT count(*) FROM raw_events WHERE event_id=?",
            Long.class, eventId)).isEqualTo(1);
        assertThat(db.queryForObject("SELECT count(*) FROM attribution_results WHERE event_id=?",
            Long.class, eventId)).isEqualTo(1);
        var storedConversionId = db.queryForObject("SELECT conversion_id FROM attribution_results WHERE event_id=?",
            UUID.class, eventId);
        assertThat(storedConversionId).isIn(first.conversionId(), second.conversionId());
        assertThat(db.queryForObject("SELECT payload FROM raw_events WHERE event_id=?",
            String.class, eventId)).isEqualTo(storedConversionId.equals(first.conversionId())
                ? firstPayload : secondPayload);
    }

    @Test
    void rollsBackRawEventWhenAttributionInsertFails() {
        var time = Instant.now();
        var event = conversion(UUID.randomUUID(), time, UUID.randomUUID()).received(time);
        db.execute("""
            CREATE FUNCTION reject_attribution_insert() RETURNS trigger AS $$
            BEGIN
                RAISE EXCEPTION 'injected attribution insert failure';
            END;
            $$ LANGUAGE plpgsql
            """);
        try {
            db.execute("""
                CREATE TRIGGER reject_attribution_insert
                BEFORE INSERT ON attribution_results
                FOR EACH ROW EXECUTE FUNCTION reject_attribution_insert()
                """);
            assertThatThrownBy(() -> store.apply(event,
                json.writeValueAsString(new Transport.Envelope(event, time))))
                .isInstanceOf(RuntimeException.class);
            assertThat(db.queryForObject("SELECT count(*) FROM raw_events WHERE event_id=?",
                Long.class, event.eventId())).isZero();
            assertThat(db.queryForObject("SELECT count(*) FROM attribution_results WHERE event_id=?",
                Long.class, event.eventId())).isZero();
        } finally {
            db.execute("DROP TRIGGER IF EXISTS reject_attribution_insert ON attribution_results");
            db.execute("DROP FUNCTION IF EXISTS reject_attribution_insert()");
        }
    }

    @Test
    void rejectsOverdueTouchAndQuarantinesPoisonRecord() throws Exception {
        var now = Instant.now();
        var user = UUID.randomUUID();
        var old = touch(user, now.minus(Duration.ofDays(3)), AdsEvent.Kind.CLICK);
        var conversion = conversion(user, now, UUID.randomUUID());
        send(old);
        kafka.send(KafkaConfiguration.EVENTS, user.toString(), "{invalid").get(10, TimeUnit.SECONDS);
        send(conversion);
        await().atMost(WAIT).untilAsserted(() -> assertThat(result(conversion).get("touch_id")).isNull());
        assertThat(db.queryForObject("SELECT eligible FROM raw_events WHERE event_id=?", Boolean.class, old.eventId()))
            .isFalse();
        try (var admin = org.apache.kafka.clients.admin.Admin.create(
            Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            await().atMost(WAIT).untilAsserted(() -> {
                var topic = KafkaConfiguration.DLQ;
                var parts = admin.describeTopics(List.of(topic)).allTopicNames().get(5, TimeUnit.SECONDS);
                var query = new java.util.HashMap<org.apache.kafka.common.TopicPartition,
                    org.apache.kafka.clients.admin.OffsetSpec>();
                parts.get(topic).partitions().forEach(p -> query.put(
                    new org.apache.kafka.common.TopicPartition(topic, p.partition()),
                    org.apache.kafka.clients.admin.OffsetSpec.latest()));
                assertThat(admin.listOffsets(query).all().get(5, TimeUnit.SECONDS).values().stream()
                    .mapToLong(o -> o.offset()).sum()).isPositive();
            });
        }
    }

    @Test
    void convergesAcrossArrivalOrdersAndChecksViewWindow() throws Exception {
        var time = Instant.now();
        var left = UUID.randomUUID();
        var right = UUID.randomUUID();
        var a = conversion(left, time, UUID.randomUUID());
        var b = conversion(right, time, UUID.randomUUID());
        var leftView = touch(left, time.minus(Duration.ofHours(25)), AdsEvent.Kind.IMPRESSION);
        var rightView = touch(right, leftView.eventTime(), AdsEvent.Kind.IMPRESSION);
        var leftClick = touch(left, time.minus(Duration.ofHours(32)), AdsEvent.Kind.CLICK);
        var rightClick = touch(right, leftClick.eventTime(), AdsEvent.Kind.CLICK);
        send(a);
        send(leftView);
        send(rightView);
        send(rightClick);
        send(b);
        awaitWinner(b, rightClick);
        await().atMost(WAIT).untilAsserted(() -> {
            assertThat(db.queryForObject("SELECT count(*) FROM raw_events WHERE user_id=?", Long.class, left))
                .isEqualTo(2);
            assertThat(result(a).get("touch_id")).isNull();
        });
        send(leftClick);
        awaitWinner(a, leftClick);
    }

    @Test
    void resumesAReplayWhoseTransactionWasInterrupted() throws Exception {
        var time = Instant.now();
        var user = UUID.randomUUID();
        var conversion = conversion(user, time, UUID.randomUUID());
        send(conversion);
        await().atMost(WAIT).untilAsserted(() -> assertThat(result(conversion).get("touch_id")).isNull());
        var touched = touch(user, time.minusSeconds(1), AdsEvent.Kind.CLICK);
        send(touched);
        awaitWinner(conversion, touched);
        var once = new AtomicInteger();
        doAnswer(call -> {
            if (once.incrementAndGet() == 2) throw new IllegalStateException("Interrupted replay transaction");
            return call.callRealMethod();
        }).when(store).lock(eq(user));
        UUID job = replay.create(time.minusSeconds(5), time.plusSeconds(5), touched.campaignId());
        await().atMost(WAIT).untilAsserted(() -> assertThat(replay.get(job).get("status")).isEqualTo("FAILED"));
        assertThat(replay.get(job).get("scanned")).isEqualTo(1L);
        assertThat(replay.resume(job)).isTrue();
        await().atMost(WAIT).untilAsserted(() -> assertThat(replay.get(job).get("status")).isEqualTo("COMPLETE"));
        assertThat(replay.get(job).get("scanned")).isEqualTo(2L);
        assertThat(replay.get(job).get("corrected")).isEqualTo(0L);
    }

    @Test
    void reportsPersistedCampaignMetricsAcrossIndependentProducers() throws Exception {
        var time = Instant.now();
        var user = UUID.randomUUID();
        var view = touch(user, time.minusSeconds(2), AdsEvent.Kind.IMPRESSION);
        var click = new AdsEvent(UUID.randomUUID(), user, AdsEvent.Kind.CLICK, view.campaignId(),
            view.adId(), null, null, null, time.minusSeconds(1), time, "integration");
        var conversion = conversion(user, time, UUID.randomUUID());
        send(view);
        try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(
                Map.of("bootstrap.servers", KAFKA.getBootstrapServers(), "acks", "all"),
                new org.apache.kafka.common.serialization.StringSerializer(),
                new org.apache.kafka.common.serialization.StringSerializer())) {
            String envelope = json.writeValueAsString(new Transport.Envelope(click, time));
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(
                KafkaConfiguration.EVENTS, user.toString(), envelope)).get(10, TimeUnit.SECONDS);
        }
        send(click);
        send(conversion);
        awaitWinner(conversion, click);
        var metrics = get("/api/v1/metrics/campaigns/" + view.campaignId());
        assertThat(metrics.statusCode()).isEqualTo(200);
        Map<?, ?> body = json.readValue(metrics.body(), Map.class);
        assertThat(body.get("impressions")).isEqualTo(1);
        assertThat(body.get("clicks")).isEqualTo(1);
        assertThat(body.get("conversions")).isEqualTo(1);
        assertThat(((Map<?, ?>) body.get("attributed_revenue")).get("USD")).isEqualTo(12.5);
        var campaigns = get("/api/v1/metrics/campaigns?campaignId=" + view.campaignId());
        assertThat(campaigns.statusCode()).isEqualTo(200);
        List<?> campaignRows = json.readValue(campaigns.body(), List.class);
        assertThat(((Map<?, ?>) campaignRows.getFirst()).get("attributed_revenue"))
            .isInstanceOf(Map.class);
        assertThat(get("/api/v1/health").statusCode()).isEqualTo(200);
    }

    @Test
    void retainsOverdueConversionsWithoutCreditingRevenue() throws Exception {
        var time = Instant.now().minus(Duration.ofDays(3));
        var conversion = conversion(UUID.randomUUID(), time, UUID.randomUUID());
        send(conversion);
        await().atMost(WAIT).untilAsserted(() -> {
            var row = result(conversion);
            assertThat(row.get("eligible")).isEqualTo(false);
            assertThat(row.get("touch_id")).isNull();
        });
    }

    private void send(AdsEvent event) throws Exception {
        assertThat(post("/api/v1/events", event).statusCode()).isEqualTo(202);
    }

    private HttpResponse<String> get(String path) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).GET().build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(String path, Object payload) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(json.writeValueAsString(payload))).build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private Map<String, Object> result(AdsEvent conversion) {
        var rows = db.queryForList("SELECT * FROM attribution_results WHERE conversion_id=?",
            conversion.conversionId());
        assertThat(rows).hasSize(1);
        return rows.getFirst();
    }

    private void awaitWinner(AdsEvent conversion, AdsEvent touch) {
        await().atMost(WAIT).untilAsserted(() -> assertThat(result(conversion).get("touch_id"))
            .isEqualTo(touch.eventId()));
    }

    private static AdsEvent conversion(UUID user, Instant time, UUID id) {
        return new AdsEvent(UUID.randomUUID(), user, AdsEvent.Kind.CONVERSION, null, null, id,
            new BigDecimal("12.50"), "USD", time, null, "integration");
    }

    private static AdsEvent touch(UUID user, Instant time, AdsEvent.Kind kind) {
        return new AdsEvent(UUID.randomUUID(), user, kind, UUID.randomUUID(), UUID.randomUUID(), null,
            null, null, time, null, "integration");
    }
}
