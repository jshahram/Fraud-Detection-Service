package frauddetectionservice.data;

import lombok.Builder;

@Builder
public record FraudAlert(
        String transactionId,
        String accountId,
        String reason,
        double amount,
        long detectedAt
) {
}