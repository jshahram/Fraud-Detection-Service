package frauddetectionservice.provider;

import frauddetectionservice.data.Account;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class AccountDataProvider {
    private final Map<String, Account> accounts = Map.of(
            "ACC-101", Account.builder()
                    .accountId("ACC-101")
                    .homeCountryCode("UK")
                    .build(),
            "ACC-102", Account.builder()
                    .accountId("ACC-102")
                    .homeCountryCode("UK")
                    .build(),
            "ACC-103", Account.builder()
                    .accountId("ACC-102")
                    .homeCountryCode("US")
                    .build(),
            "ACC-104", Account.builder()
                    .accountId("ACC-103")
                    .homeCountryCode("DE")
                    .build(),
            "ACC-105", Account.builder()
                    .accountId("ACC-104")
                    .homeCountryCode("FR")
                    .build()
    );

    public Map<String, Account> getAllAccounts() {
        return accounts;
    }

    public Account getRandomAccount() {
        List<String> accountIds = List.copyOf(accounts.keySet());
        String randomId = accountIds.get(ThreadLocalRandom.current().nextInt(accountIds.size()));
        return accounts.get(randomId);
    }
}