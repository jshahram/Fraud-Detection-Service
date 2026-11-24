package com.shahramjaved.frauddetectionservice.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
    @Bean
    public NewTopic transactionsRawTopic() {
        return TopicBuilder
                .name("transactions-raw")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic accountsTopic() {
        return TopicBuilder
                .name("accounts")
                .partitions(3)
                .replicas(1)
                .compact()
                .build();
    }

    @Bean
    public NewTopic merchantsTopic() {
        return TopicBuilder
                .name("merchants")
                .partitions(3)
                .replicas(1)
                .compact()
                .build();
    }

    @Bean
    public NewTopic fraudAlertsTopic() {
        return TopicBuilder
                .name("fraud-alerts")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
