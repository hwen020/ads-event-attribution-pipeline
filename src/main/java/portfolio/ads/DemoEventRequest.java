package portfolio.ads;

import java.time.Instant;

record DemoEventRequest(AdsEvent event, Instant simulatedReceivedAt) {}
