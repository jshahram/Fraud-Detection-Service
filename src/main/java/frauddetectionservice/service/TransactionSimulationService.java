package frauddetectionservice.service;

import frauddetectionservice.data.Account;
import frauddetectionservice.data.Merchant;
import frauddetectionservice.data.Transaction;
import frauddetectionservice.provider.AccountDataProvider;
import frauddetectionservice.provider.CountryDataProvider;
import frauddetectionservice.provider.MerchantDataProvider;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Service
@Slf4j
@ConditionalOnProperty(name = "fraud.simulation.enabled", havingValue = "true", matchIfMissing = true)
public class TransactionSimulationService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final AccountDataProvider accountDataProvider;
    private final MerchantDataProvider merchantDataProvider;
    private final CountryDataProvider countryDataProvider;

    public TransactionSimulationService(
            KafkaTemplate<String, Object> kafkaTemplate,
            AccountDataProvider accountDataProvider,
            MerchantDataProvider merchantDataProvider,
            CountryDataProvider countryDataProvider) {

        this.kafkaTemplate = kafkaTemplate;
        this.accountDataProvider = accountDataProvider;
        this.merchantDataProvider = merchantDataProvider;
        this.countryDataProvider = countryDataProvider;
    }

    @PostConstruct
    public void seedReferenceData() {
        accountDataProvider.getAllAccounts().forEach((accountId, account) -> {
            kafkaTemplate.send("accounts", accountId, account);
            log.info("Seeded account {}", accountId);
        });

        merchantDataProvider.getAllMerchants().forEach((merchantId, merchant) -> {
            kafkaTemplate.send("merchants", merchantId, merchant);
            log.info("Seeded merchant {} (blacklisted={})", merchantId, merchant.isBlacklisted());
        });
    }

    @Scheduled(fixedRate = 1000)
    public void simulateTransactions() {
        int scenario = ThreadLocalRandom.current().nextInt(20);
        Account account = accountDataProvider.getRandomAccount();

        if (scenario == 0) {
            // 5% chance: High value spike
            Merchant safeMerchant = merchantDataProvider.getRandomSafeMerchant();
            buildAndSend(account.accountId(), 6500.00, safeMerchant.merchantId(), account.homeCountryCode(), "HIGH VALUE SPIKE");

        } else if (scenario == 1) {
            // 5% chance: Impossible travel
            Merchant homeMerchant = merchantDataProvider.getRandomSafeMerchant();
            buildAndSend(account.accountId(), 28.00, homeMerchant.merchantId(), account.homeCountryCode(), "IMPOSSIBLE TRAVEL (LEG 1 - HOME)");

            Merchant foreignMerchant = merchantDataProvider.getRandomSafeMerchant();
            String foreignCountryCode = countryDataProvider.getRandomForeignCountryCode(account.homeCountryCode());
            buildAndSend(account.accountId(), 120.00, foreignMerchant.merchantId(), foreignCountryCode, "IMPOSSIBLE TRAVEL (LEG 2 - FOREIGN)");

        } else if (scenario == 2) {
            // 5% chance: Blacklisted merchant
            Merchant blacklistedMerchant = merchantDataProvider.getRandomBlacklistedMerchant();
            buildAndSend(account.accountId(), 45.00, blacklistedMerchant.merchantId(), account.homeCountryCode(), "BLACKLISTED MERCHANT");

        } else if (scenario == 3) {
            // 5% chance: Velocity burst (5 rapid swipes with unique IDs)
            log.warn("Simulating VELOCITY BURST for {}", account.accountId());
            for (int i = 0; i < 5; i++) {
                Merchant safeMerchant = merchantDataProvider.getRandomSafeMerchant();
                buildAndSend(account.accountId(), 15.00 + i, safeMerchant.merchantId(), account.homeCountryCode(), "VELOCITY SWIPE");
            }

        } else {
            // 80% chance: Normal everyday transaction
            Merchant safeMerchant = merchantDataProvider.getRandomSafeMerchant();
            double normalAmount = Math.round((5.00 + ThreadLocalRandom.current().nextDouble(150.00)) * 100.0) / 100.0;
            buildAndSend(account.accountId(), normalAmount, safeMerchant.merchantId(), account.homeCountryCode(), "NORMAL");
        }
    }

    private void buildAndSend(String accountId, double amount, String merchantId, String countryCode, String tag) {
        Transaction transaction = Transaction.builder()
                .transactionId(UUID.randomUUID().toString())
                .accountId(accountId)
                .amount(amount)
                .merchantId(merchantId)
                .countryCode(countryCode)
                .currency("GBP")
                .timestamp(Instant.now().toEpochMilli())
                .build();

        kafkaTemplate.send("transactions-raw", transaction.accountId(), transaction);
        log.info("[{}] Sent transaction {} for {}: £{} at {} ({})",
                tag, transaction.transactionId(), transaction.accountId(), transaction.amount(), transaction.merchantId(), transaction.countryCode());
    }
}
