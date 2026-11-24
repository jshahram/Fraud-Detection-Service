package com.shahramjaved.frauddetectionservice.util;

import lombok.experimental.UtilityClass;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

@UtilityClass
public class KafkaUtil {
    public <T> JacksonJsonSerde<T> createJsonSerde(Class<T> targetType) {
        return new JacksonJsonSerde<>(targetType).ignoreTypeHeaders().noTypeInfo();
    }
}
