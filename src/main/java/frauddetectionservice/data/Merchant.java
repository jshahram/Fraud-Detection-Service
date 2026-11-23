package frauddetectionservice.data;

import lombok.Builder;

@Builder
public record Merchant(
        String merchantId,
        boolean isBlacklisted
) {
}