package com.vay.h1.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vay.h1.model.WeatherInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.List;
import java.util.Random;

@Slf4j
@Component
@RequiredArgsConstructor
public class WeatherProducer {
    private static final List<String> CITIES = List.of("Магадан", "Чукотка", "Питер", "Тюмень");
    private static final List<String> CONDITIONS = List.of("солнечно", "облачно", "дождь");
    private static final Random RANDOM = new Random();

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Scheduled(fixedRate = 10000)
    public void sendWeather() throws JsonProcessingException {
        for (String city : CITIES) {
            WeatherInfo info = new WeatherInfo(
                    city,
                    LocalDate.now().minusDays(RANDOM.nextInt(7)),
                    RANDOM.nextInt(36),
                    CONDITIONS.get(RANDOM.nextInt(CONDITIONS.size()))
            );
            String message = objectMapper.writeValueAsString(info);
            kafkaTemplate.send("weather", city, message);
            log.info("Отправлено: {}", message);
        }
    }
} 