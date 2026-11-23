package frauddetectionservice.provider;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class CountryDataProvider {
    private final List<String> countryCodes = List.of("UK", "US", "DE", "FR", "JP", "AU", "BR");

    public String getRandomForeignCountryCode(String homeCountryCode) {
        List<String> foreignCountryCodes = countryCodes.stream()
                .filter(country -> !country.equalsIgnoreCase(homeCountryCode))
                .toList();

        return foreignCountryCodes.get(ThreadLocalRandom.current().nextInt(foreignCountryCodes.size()));
    }
}
