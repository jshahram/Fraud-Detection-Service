package frauddetectionservice.data;

import lombok.Builder;

@Builder
public record Transaction(
        String transactionId,
        String accountId,
        double amount,
        String currency,
        String merchantId,
        String countryCode,
        long timestamp
){};