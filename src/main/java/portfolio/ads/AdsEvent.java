package portfolio.ads;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record AdsEvent(
    @NotNull UUID eventId, @NotNull UUID userId, @NotNull Kind type,
    UUID campaignId, UUID adId, UUID conversionId,
    @PositiveOrZero @Digits(integer = 16, fraction = 4) BigDecimal conversionValue,
    @Pattern(regexp = "[A-Z]{3}") String currency,
    @NotNull Instant eventTime, @JsonProperty(access = JsonProperty.Access.READ_ONLY) Instant receivedAt,
    @NotBlank @Size(max = 48) String source
) {
    public enum Kind { IMPRESSION, CLICK, CONVERSION }

    @com.fasterxml.jackson.annotation.JsonIgnore
    @AssertTrue(message = "Conversion and touch fields must match the event type")
    public boolean isShapeValid() {
        if (type == Kind.CONVERSION) {
            return conversionId != null && conversionValue != null && currency != null
                && campaignId == null && adId == null;
        }
        return campaignId != null && adId != null
            && conversionId == null && conversionValue == null && currency == null;
    }

    AdsEvent received(Instant now) {
        return new AdsEvent(eventId, userId, type, campaignId, adId, conversionId,
            conversionValue, currency, eventTime, now, source);
    }

    boolean eligible() {
        return !receivedAt.isAfter(eventTime.plus(Attribution.LATENESS));
    }
}
