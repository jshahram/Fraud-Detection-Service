package frauddetectionservice.data;

import lombok.Builder;

@Builder
public record Account(
        String accountId,
        String homeCountryCode
) {
}