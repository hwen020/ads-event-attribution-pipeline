package portfolio.ads;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("public-demo")
class DemoPublicProfileTest {
    private static final UUID SEARCH = UUID.fromString("10000000-0000-4000-8000-000000000001");
    private static final UUID DISPLAY = UUID.fromString("10000000-0000-4000-8000-000000000002");
    private static final UUID SOCIAL = UUID.fromString("10000000-0000-4000-8000-000000000003");
    @Autowired MockMvc mvc;
    @Autowired ObjectMapper json;

    @Test
    void publicProfileStartsWithoutKafkaOrPostgresAndExposesActualMode() throws Exception {
        mvc.perform(get("/api/v1/demo/health"))
            .andExpect(status().isOk())
            .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath("$.mode").value("public-demo"))
            .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath("$.transport").value("synchronous-in-process"))
            .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath("$.storage").value("ephemeral-in-memory"));
        mvc.perform(get("/"))
            .andExpect(status().isOk())
            .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.content().string(
                org.hamcrest.Matchers.containsString("Public demo mode")));
        mvc.perform(get("/demo-assets/demo.js")).andExpect(status().isOk());
        mvc.perform(get("/api/v1/health")).andExpect(status().isNotFound());
    }

    @Test
    void lateTouchDuplicateFutureClickWindowAndConflictAreVisibleThroughHttp() throws Exception {
        String session = newSession();
        UUID user = state(session).get("userId").asString();
        Instant anchor = Instant.now().minus(Duration.ofMinutes(2));
        UUID impressionId = UUID.randomUUID();
        UUID conversionId = UUID.randomUUID();

        send(session, event(impressionId, user, AdsEvent.Kind.IMPRESSION, DISPLAY,
            anchor.minus(Duration.ofMinutes(25)), null));
        send(session, event(UUID.randomUUID(), user, AdsEvent.Kind.CLICK, SEARCH,
            anchor.minus(Duration.ofMinutes(15)), null));
        var conversion = event(UUID.randomUUID(), user, AdsEvent.Kind.CONVERSION, null,
            anchor.minus(Duration.ofMinutes(10)), conversionId);
        conversion = new AdsEvent(conversion.eventId(), conversion.userId(), conversion.type(),
            conversion.campaignId(), conversion.adId(), conversion.conversionId(),
            new BigDecimal("42.50"), "USD", conversion.eventTime(), null, conversion.source());
        send(session, conversion);

        var initial = state(session);
        assertThat(initial.get("conversionCount").asInt()).isEqualTo(1);
        assertThat(initial.at("/attributions/0/campaignId").asString()).isEqualTo(SEARCH.toString());
        assertThat(post(session, conversion).get("outcome").asString()).isEqualTo("DUPLICATE");
        assertThat(state(session).get("conversionCount").asInt()).isEqualTo(1);

        var lateClick = event(UUID.randomUUID(), user, AdsEvent.Kind.CLICK, DISPLAY,
            anchor.minus(Duration.ofMinutes(12)), null);
        assertThat(post(session, lateClick, anchor.minus(Duration.ofMinutes(8)))
            .get("outcome").asString()).isEqualTo("ACCEPTED");
        assertThat(state(session).at("/attributions/0/campaignId").asString()).isEqualTo(DISPLAY.toString());
        var futureClick = event(UUID.randomUUID(), user, AdsEvent.Kind.CLICK, SOCIAL,
            anchor.plus(Duration.ofMinutes(2)), null);
        send(session, futureClick);
        assertThat(state(session).at("/attributions/0/campaignId").asString()).isEqualTo(DISPLAY.toString());

        var oldClick = event(UUID.randomUUID(), user, AdsEvent.Kind.CLICK, SEARCH,
            anchor.minus(Duration.ofDays(10)), null);
        assertThat(post(session, oldClick).get("outcome").asString()).isEqualTo("ACCEPTED");
        var noTouchPurchase = event(UUID.randomUUID(), user, AdsEvent.Kind.CONVERSION, null,
            anchor.minus(Duration.ofHours(47)), UUID.randomUUID());
        noTouchPurchase = new AdsEvent(noTouchPurchase.eventId(), noTouchPurchase.userId(),
            noTouchPurchase.type(), null, null, noTouchPurchase.conversionId(),
            new BigDecimal("42.50"), "USD", noTouchPurchase.eventTime(), null, noTouchPurchase.source());
        send(session, noTouchPurchase);
        var afterNoTouch = state(session);
        assertThat(afterNoTouch.get("conversionCount").asInt()).isEqualTo(2);
        assertThat(afterNoTouch.at("/metrics/unattributedConversions").asInt()).isEqualTo(1);
        assertThat(afterNoTouch.at("/attributions/1/status").asString()).isEqualTo("UNATTRIBUTED");

        var latePurchase = event(UUID.randomUUID(), user, AdsEvent.Kind.CONVERSION, null,
            anchor.minus(Duration.ofHours(50)), UUID.randomUUID());
        assertThat(post(session, latePurchase, latePurchase.eventTime().plus(Duration.ofHours(49)))
            .get("outcome").asString()).isEqualTo("LATE");
        var afterLatePurchase = state(session);
        assertThat(afterLatePurchase.get("conversionCount").asInt()).isEqualTo(3);
        assertThat(afterLatePurchase.at("/metrics/unattributedConversions").asInt()).isEqualTo(2);
        assertThat(afterLatePurchase.at("/attributions/2/status").asString()).isEqualTo("LATE");
        assertThat(afterLatePurchase.at("/attributions/2/touchId").isNull()).isTrue();

        var conflict = new AdsEvent(impressionId, user, AdsEvent.Kind.IMPRESSION, SOCIAL,
            UUID.randomUUID(), null, null, null, anchor.minus(Duration.ofMinutes(25)), null, "public-demo-test");
        assertThat(post(session, conflict).get("outcome").asString()).isEqualTo("CONFLICT");
        var campaignRows = state(session).at("/metrics/campaigns");
        assertThat(campaignRows.has(SEARCH.toString())).isTrue();
        assertThat(campaignRows.has(DISPLAY.toString())).isTrue();
        assertThat(campaignRows.has(SOCIAL.toString())).isTrue();
        assertThat(campaignRows.at("/" + SOCIAL + "/impressions").asInt()).isZero();
        assertThat(campaignRows.at("/" + SOCIAL + "/clicks").asInt()).isEqualTo(1);
    }

    @Test
    void independentSessionsCannotReadOrResetEachOthersData() throws Exception {
        String first = newSession();
        String second = newSession();
        UUID firstUser = state(first).get("userId").asString();
        UUID secondUser = state(second).get("userId").asString();
        assertThat(first).isNotEqualTo(second);

        send(first, event(UUID.randomUUID(), firstUser, AdsEvent.Kind.IMPRESSION, DISPLAY, Instant.now(), null));
        send(second, event(UUID.randomUUID(), secondUser, AdsEvent.Kind.CLICK, SEARCH, Instant.now(), null));

        mvc.perform(get("/api/v1/demo/state").header("X-Demo-Session", second))
            .andExpect(status().isOk())
            .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath("$.eventCount").value(1));
        mvc.perform(post("/api/v1/demo/reset").header("X-Demo-Session", first))
            .andExpect(status().isOk())
            .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath("$.eventCount").value(0));
        assertThat(state(second).get("eventCount").asInt()).isEqualTo(1);

        var wrongOwner = event(UUID.randomUUID(), secondUser, AdsEvent.Kind.IMPRESSION, DISPLAY, Instant.now(), null);
        mvc.perform(post("/api/v1/demo/events").header("X-Demo-Session", first)
                .contentType(MediaType.APPLICATION_JSON)
                .content(payload(wrongOwner, wrongOwner.eventTime().plusSeconds(60))))
            .andExpect(status().isForbidden());
    }

    @Test
    void concurrentStableRetriesOnlyCreateOneEventAndOversizedPayloadIsRejected() throws Exception {
        String session = newSession();
        UUID user = state(session).get("userId").asString();
        var stable = event(UUID.randomUUID(), user, AdsEvent.Kind.IMPRESSION, DISPLAY, Instant.now(), null);
        try (var pool = Executors.newFixedThreadPool(20)) {
            var tasks = new ArrayList<java.util.concurrent.Future<MvcResult>>();
            for (int i = 0; i < 20; i++) {
                tasks.add(pool.submit(() -> mvc.perform(post("/api/v1/demo/events")
                    .header("X-Demo-Session", session)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(payload(stable, stable.eventTime().plusSeconds(60)))).andReturn()));
            }
            var outcomes = new ArrayList<String>();
            for (var task : tasks) {
                var result = task.get();
                assertThat(result.getResponse().getStatus()).isEqualTo(200);
                outcomes.add(json.readTree(result.getResponse().getContentAsByteArray()).get("outcome").asString());
            }
            assertThat(outcomes).containsExactlyInAnyOrder("ACCEPTED",
                "DUPLICATE", "DUPLICATE", "DUPLICATE", "DUPLICATE", "DUPLICATE", "DUPLICATE",
                "DUPLICATE", "DUPLICATE", "DUPLICATE", "DUPLICATE", "DUPLICATE", "DUPLICATE",
                "DUPLICATE", "DUPLICATE", "DUPLICATE", "DUPLICATE", "DUPLICATE", "DUPLICATE",
                "DUPLICATE");
        }
        assertThat(state(session).get("eventCount").asInt()).isEqualTo(1);
        mvc.perform(post("/api/v1/demo/events").header("X-Demo-Session", session)
                .contentType(MediaType.APPLICATION_JSON).content(" ".repeat(16_385)))
            .andExpect(status().isPayloadTooLarge());
    }

    @Test
    void sessionExpiresAfterTwoHoursOfInactivity() {
        var clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        var store = new DemoSessionStore(clock);
        var created = store.create();
        clock.advance(DemoSessionStore.SESSION_TTL.plusMillis(1));
        assertThatThrownBy(() -> store.state(created.sessionId()))
            .isInstanceOf(ResponseStatusException.class)
            .extracting(error -> ((ResponseStatusException) error).getStatusCode().value())
            .isEqualTo(404);
    }

    @Test
    void sessionEventLimitRejectsTheOneHundredAndFirstUniqueEvent() {
        var clock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneId.of("UTC"));
        var store = new DemoSessionStore(clock);
        var created = store.create();
        for (int i = 0; i < DemoSessionStore.MAX_EVENTS_PER_SESSION; i++) {
            var eventTime = clock.instant().minusSeconds(i);
            store.ingest(created.sessionId(), event(UUID.randomUUID(), created.userId(),
                AdsEvent.Kind.IMPRESSION, DISPLAY, eventTime, null), eventTime.plusSeconds(1));
        }
        var eventTime = clock.instant();
        assertThatThrownBy(() -> store.ingest(created.sessionId(), event(UUID.randomUUID(), created.userId(),
            AdsEvent.Kind.IMPRESSION, DISPLAY, eventTime, null), eventTime.plusSeconds(1)))
            .isInstanceOf(ResponseStatusException.class)
            .extracting(error -> ((ResponseStatusException) error).getStatusCode().value())
            .isEqualTo(429);
        assertThat(store.state(created.sessionId()).eventCount()).isEqualTo(DemoSessionStore.MAX_EVENTS_PER_SESSION);
    }

    private String newSession() throws Exception {
        var response = mvc.perform(post("/api/v1/demo/sessions")).andExpect(status().isOk()).andReturn();
        return json.readTree(response.getResponse().getContentAsByteArray()).get("sessionId").asString();
    }

    private tools.jackson.databind.JsonNode state(String session) throws Exception {
        var response = mvc.perform(get("/api/v1/demo/state").header("X-Demo-Session", session))
            .andExpect(status().isOk()).andReturn();
        return json.readTree(response.getResponse().getContentAsByteArray());
    }

    private tools.jackson.databind.JsonNode post(String session, AdsEvent event) throws Exception {
        return post(session, event, event.eventTime().plusSeconds(60));
    }

    private tools.jackson.databind.JsonNode post(String session, AdsEvent event, Instant simulatedReceivedAt) throws Exception {
        var response = mvc.perform(post("/api/v1/demo/events").header("X-Demo-Session", session)
                .contentType(MediaType.APPLICATION_JSON).content(payload(event, simulatedReceivedAt)))
            .andExpect(status().isOk()).andReturn();
        return json.readTree(response.getResponse().getContentAsByteArray());
    }

    private void send(String session, AdsEvent event) throws Exception {
        post(session, event);
    }

    private byte[] payload(AdsEvent event, Instant simulatedReceivedAt) throws Exception {
        return json.writeValueAsBytes(java.util.Map.of(
            "event", event, "simulatedReceivedAt", simulatedReceivedAt));
    }

    private static AdsEvent event(UUID id, UUID user, AdsEvent.Kind kind, UUID campaign, Instant time,
                                  UUID conversion) {
        if (kind == AdsEvent.Kind.CONVERSION) {
            return new AdsEvent(id, user, kind, null, null, conversion, new BigDecimal("42.50"),
                "USD", time, null, "public-demo-test");
        }
        return new AdsEvent(id, user, kind, campaign, UUID.randomUUID(), null, null,
            null, time, null, "public-demo-test");
    }

    private static final class MutableClock extends Clock {
        private Instant now;

        private MutableClock(Instant now) {
            this.now = now;
        }

        private void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override public ZoneId getZone() { return ZoneId.of("UTC"); }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }
}
