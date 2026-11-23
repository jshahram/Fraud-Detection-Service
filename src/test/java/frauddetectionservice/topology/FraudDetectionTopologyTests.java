package frauddetectionservice.topology;

import frauddetectionservice.data.Account;
import frauddetectionservice.data.FraudAlert;
import frauddetectionservice.data.Merchant;
import frauddetectionservice.data.Transaction;
import frauddetectionservice.util.KafkaUtil;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FraudDetectionTopologyTests {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Account> accounts;
    private TestInputTopic<String, Merchant> merchants;
    private TestInputTopic<String, Transaction> transactions;
    private TestOutputTopic<String, FraudAlert> fraudAlerts;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        new FraudDetectionTopology().fraudDetectionStream(builder);

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-detection-topology-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);

        Topology topology = builder.build();
        testDriver = new TopologyTestDriver(topology, properties, Instant.EPOCH);

        JacksonJsonSerde<Account> accountSerde = KafkaUtil.createJsonSerde(Account.class);
        JacksonJsonSerde<Merchant> merchantSerde = KafkaUtil.createJsonSerde(Merchant.class);
        JacksonJsonSerde<Transaction> transactionSerde = KafkaUtil.createJsonSerde(Transaction.class);
        JacksonJsonSerde<FraudAlert> fraudAlertSerde = KafkaUtil.createJsonSerde(FraudAlert.class);

        accounts = testDriver.createInputTopic(
                "accounts", Serdes.String().serializer(), accountSerde.serializer());
        merchants = testDriver.createInputTopic(
                "merchants", Serdes.String().serializer(), merchantSerde.serializer());
        transactions = testDriver.createInputTopic(
                "transactions-raw", Serdes.String().serializer(), transactionSerde.serializer());
        fraudAlerts = testDriver.createOutputTopic(
                "fraud-alerts", Serdes.String().deserializer(), fraudAlertSerde.deserializer());

        accounts.pipeInput("ACC-1", Account.builder()
                .accountId("ACC-1")
                .homeCountryCode("UK")
                .build());
        merchants.pipeInput("SAFE", Merchant.builder()
                .merchantId("SAFE")
                .isBlacklisted(false)
                .build());
        merchants.pipeInput("BLOCKED", Merchant.builder()
                .merchantId("BLOCKED")
                .isBlacklisted(true)
                .build());
    }

    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    void emitsHighValueAlert() {
        transactions.pipeInput("ACC-1", transaction("TX-HIGH", "ACC-1", 6500.00, "SAFE", "UK"));

        FraudAlert alert = fraudAlerts.readValue();
        assertEquals("TX-HIGH", alert.transactionId());
        assertEquals("ACC-1", alert.accountId());
        assertEquals("HIGH VALUE SPIKE", alert.reason());
        assertTrue(fraudAlerts.isEmpty());
    }

    @Test
    void emitsBlacklistedMerchantAlert() {
        transactions.pipeInput("ACC-1", transaction("TX-BLOCKED", "ACC-1", 45.00, "BLOCKED", "UK"));

        FraudAlert alert = fraudAlerts.readValue();
        assertEquals("TX-BLOCKED", alert.transactionId());
        assertEquals("BLACKLISTED MERCHANT", alert.reason());
    }

    @Test
    void emitsVelocityAlertAfterMoreThanThreeTransactionsInWindow() {
        for (int i = 1; i <= 4; i++) {
            transactions.pipeInput("ACC-1", transaction("TX-" + i, "ACC-1", 20.00, "SAFE", "UK"));
        }

        FraudAlert alert = fraudAlerts.readValue();
        assertEquals("ACC-1", alert.accountId());
        assertEquals("VELOCITY SWIPE", alert.reason());
    }

    @Test
    void emitsImpossibleTravelAlert() {
        transactions.pipeInput("ACC-1", transaction("TX-LEG-1 (UK)", "ACC-1", 25.00, "SAFE", "UK"));
        transactions.pipeInput("ACC-1", transaction("TX-LEG-2 (US)", "ACC-1", 75.00, "SAFE", "US"));

        FraudAlert alert = fraudAlerts.readValue();
        assertEquals("ACC-1", alert.accountId());
        assertEquals("IMPOSSIBLE TRAVEL", alert.reason());
    }

    private Transaction transaction(
            String transactionId,
            String accountId,
            double amount,
            String merchantId,
            String countryCode) {

        return Transaction.builder()
                .transactionId(transactionId)
                .accountId(accountId)
                .amount(amount)
                .currency("GBP")
                .merchantId(merchantId)
                .countryCode(countryCode)
                .timestamp(Instant.EPOCH.toEpochMilli())
                .build();
    }
}
