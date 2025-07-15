package com.vay.h1.kafka;

import com.vay.h1.model.WeatherInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class WeatherConsumer {
    @KafkaListener(topics = "weather", groupId = "weather-group")
    public void listener(ConsumerRecord<String, WeatherInfo> record) {
        String city = record.key();
        WeatherInfo info = record.value();
        if (info == null) {
            log.warn("Получено пустое сообщение для города: {}", city);
            return;
        }
        String data = info.toString();
        log.info("Получено: {}", data);
    }
} 