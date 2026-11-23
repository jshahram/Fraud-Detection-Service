package frauddetectionservice.provider;

import frauddetectionservice.data.Merchant;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class MerchantDataProvider {
    private final Map<String, Merchant> safeMerchants = Map.of(
            "MERCH-AMZ", Merchant.builder()
                    .merchantId("MERCH-AMZ")
                    .isBlacklisted(false)
                    .build(),
            "MERCH-TSCO", Merchant.builder()
                    .merchantId("MERCH-TSCO")
                    .isBlacklisted(false)
                    .build(),
            "MERCH-AAPL", Merchant.builder()
                    .merchantId("MERCH-AAPL")
                    .isBlacklisted(false)
                    .build(),
            "MERCH-UBER", Merchant.builder()
                    .merchantId("MERCH-UBER")
                    .isBlacklisted(false)
                    .build()
    );

    private final Map<String, Merchant> blacklistedMerchants = Map.of(
            "MERCH-SHADY-999", Merchant.builder()
                    .merchantId("MERCH-SHADY-999")
                    .isBlacklisted(true)
                    .build(),
            "MERCH-FAKE-CRYPTO", Merchant.builder()
                    .merchantId("MERCH-FAKE-CRYPTO")
                    .isBlacklisted(true)
                    .build()
    );

    public Map<String, Merchant> getAllMerchants() {
        Map<String, Merchant> allMerchants = new HashMap<>(safeMerchants);
        allMerchants.putAll(blacklistedMerchants);
        return Collections.unmodifiableMap(allMerchants);
    }

    public Merchant getRandomSafeMerchant() {
        List<String> merchantIds = List.copyOf(safeMerchants.keySet());
        String randomId = merchantIds.get(ThreadLocalRandom.current().nextInt(merchantIds.size()));
        return safeMerchants.get(randomId);
    }

    public Merchant getRandomBlacklistedMerchant() {
        List<String> merchantIds = List.copyOf(blacklistedMerchants.keySet());
        String randomId = merchantIds.get(ThreadLocalRandom.current().nextInt(merchantIds.size()));
        return blacklistedMerchants.get(randomId);
    }
}
