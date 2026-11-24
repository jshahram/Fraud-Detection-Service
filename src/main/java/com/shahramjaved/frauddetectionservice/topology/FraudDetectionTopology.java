package com.shahramjaved.frauddetectionservice.topology;

import com.shahramjaved.frauddetectionservice.data.*;
import com.shahramjaved.frauddetectionservice.util.KafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

@Configuration
@EnableKafkaStreams
@Slf4j
public class FraudDetectionTopology {
    @Bean
    public KStream<String, Transaction> fraudDetectionStream(StreamsBuilder builder) {
        JacksonJsonSerde<Transaction> transactionSerde = KafkaUtil.createJsonSerde(Transaction.class);
        JacksonJsonSerde<Account> accountSerde = KafkaUtil.createJsonSerde(Account.class);
        JacksonJsonSerde<Merchant> merchantSerde = KafkaUtil.createJsonSerde(Merchant.class);
        JacksonJsonSerde<LocationState> locationStateSerde = KafkaUtil.createJsonSerde(LocationState.class);
        JacksonJsonSerde<FraudAlert> alertSerde = KafkaUtil.createJsonSerde(FraudAlert.class);

        KStream<String, Transaction> transactions = builder.stream(
                "transactions-raw",
                Consumed.with(Serdes.String(), transactionSerde)
        );

        KTable<String, Account> accounts = builder.table(
                "accounts",
                Consumed.with(Serdes.String(), accountSerde)
        );

        GlobalKTable<String, Merchant> merchants = builder.globalTable(
                "merchants",
                Consumed.with(Serdes.String(), merchantSerde)
        );

        KStream<String, FraudAlert> accountRuleAlerts = transactions
                .join(
                        accounts,
                        this::evaluateAccountRules,
                        Joined.with(Serdes.String(), transactionSerde, accountSerde)
                )
                .filter((accountId, optionalAlert) -> optionalAlert.isPresent())
                .mapValues(Optional::get);

        KStream<String, FraudAlert> merchantRuleAlerts = transactions
                .join(
                        merchants,
                        (accountId, transaction) -> transaction.merchantId(),
                        this::evaluateMerchantRules
                )
                .filter((accountId, optionalAlert) -> optionalAlert.isPresent())
                .mapValues(Optional::get);

        KStream<String, FraudAlert> velocityAlerts = transactions
                .groupByKey(Grouped.with(Serdes.String(), transactionSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                .count(Materialized.as("transaction-velocity-store"))
                .toStream()
                .map((windowedKey, count) -> KeyValue.pair(
                        windowedKey.key(),
                        evaluateVelocityRule(windowedKey.key(), count)
                ))
                .filter((accountId, optionalAlert) -> optionalAlert.isPresent())
                .mapValues(Optional::get);

        KStream<String, FraudAlert> locationAlerts = transactions
                .groupByKey(Grouped.with(Serdes.String(), transactionSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                .aggregate(() -> LocationState.builder().build(),
                        (accountId, transaction, locationState) -> LocationState.builder()
                                .previousCountryCode(locationState.currentCountryCode())
                                .currentCountryCode(transaction.countryCode())
                                .timestamp(transaction.timestamp())
                                .build(),
                        Materialized.<String, LocationState, WindowStore<Bytes, byte[]>>as("transaction-location-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(locationStateSerde))
                .toStream()
                .map((windowedKey, locationState) -> KeyValue.pair(
                        windowedKey.key(),
                        evaluateLocationRule(windowedKey.key(), locationState)
                ))
                .filter((accountId, optionalAlert) -> optionalAlert.isPresent())
                .mapValues(Optional::get);

        accountRuleAlerts
                .merge(merchantRuleAlerts)
                .merge(velocityAlerts)
                .merge(locationAlerts)
                .peek((accountId, alert) -> log.warn("FRAUD ALERT DETECTED: {}", alert))
                .to("fraud-alerts", Produced.with(Serdes.String(), alertSerde));

        return transactions;
    }

    private Optional<FraudAlert> evaluateAccountRules(Transaction transaction, Account account) {
        if (transaction.amount() >= 5000.00) {
            return Optional.of(FraudAlert.builder()
                    .transactionId(transaction.transactionId())
                    .accountId(transaction.accountId())
                    .amount(transaction.amount())
                    .reason("HIGH VALUE SPIKE")
                    .detectedAt(Instant.now().toEpochMilli())
                    .build());
        }
        return Optional.empty();
    }

    private Optional<FraudAlert> evaluateMerchantRules(Transaction transaction, Merchant merchant) {
        if (merchant.isBlacklisted()) {
            return Optional.of(FraudAlert.builder()
                    .transactionId(transaction.transactionId())
                    .accountId(transaction.accountId())
                    .amount(transaction.amount())
                    .reason("BLACKLISTED MERCHANT")
                    .detectedAt(Instant.now().toEpochMilli())
                    .build());
        }
        return Optional.empty();
    }

    private Optional<FraudAlert> evaluateVelocityRule(String accountId, Long transactionCount) {
        if (transactionCount > 3) {
            return Optional.of(FraudAlert.builder()
                    .accountId(accountId)
                    .reason("VELOCITY SWIPE")
                    .detectedAt(Instant.now().toEpochMilli())
                    .build());
        }
        return Optional.empty();
    }

    private Optional<FraudAlert> evaluateLocationRule(String accountId, LocationState locationState) {
        if (locationState.previousCountryCode() != null &&
                !locationState.previousCountryCode().equals(locationState.currentCountryCode())) {

            return Optional.of(FraudAlert.builder()
                    .accountId(accountId)
                    .reason("IMPOSSIBLE TRAVEL")
                    .detectedAt(Instant.now().toEpochMilli())
                    .build());
        }
        return Optional.empty();
    }
}