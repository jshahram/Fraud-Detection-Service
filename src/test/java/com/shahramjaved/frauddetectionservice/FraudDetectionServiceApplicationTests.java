package com.shahramjaved.frauddetectionservice;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
        "fraud.simulation.enabled=false",
        "spring.kafka.streams.auto-startup=false"
})
class FraudDetectionServiceApplicationTests {
    @Test
    void contextLoads() {
    }
}
