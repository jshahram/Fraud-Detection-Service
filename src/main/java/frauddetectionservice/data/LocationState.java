package frauddetectionservice.data;

import lombok.Builder;

@Builder
public record LocationState(
        String previousCountryCode,
        String currentCountryCode,
        long timestamp
) {
}