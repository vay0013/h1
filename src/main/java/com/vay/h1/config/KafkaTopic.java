package com.vay.h1.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopic {
    @Bean
    public NewTopic weather() {
        return TopicBuilder.name("weather")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
