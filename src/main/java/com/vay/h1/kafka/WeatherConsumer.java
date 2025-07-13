package com.vay.h1.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vay.h1.model.WeatherInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class WeatherConsumer {
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "weather", groupId = "weather-group")
    public void listen(ConsumerRecord<String, String> record) throws Exception {
        WeatherInfo info = objectMapper.readValue(record.value(), WeatherInfo.class);
        log.info("Получено: {}", info);
    }
} 