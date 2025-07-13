package com.vay.h1.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vay.h1.model.WeatherInfo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class WeatherConsumerTest {

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private ConsumerRecord<String, String> consumerRecord;

    private WeatherConsumer weatherConsumer;

    @BeforeEach
    void setUp() {
        weatherConsumer = new WeatherConsumer(objectMapper);
    }

    @Test
    void testListenSuccess() throws Exception {
        String jsonMessage = """
                {
                    "city":"Москва",
                    "date":"2024-01-15",
                    "temperature":25,
                    "condition":"солнечно"
                }""";
        WeatherInfo expectedWeatherInfo = new WeatherInfo(
                "Москва",
                LocalDate.of(2024, 1, 15),
                25,
                "солнечно");
        when(consumerRecord.value()).thenReturn(jsonMessage);
        when(objectMapper.readValue(jsonMessage, WeatherInfo.class)).thenReturn(expectedWeatherInfo);

        weatherConsumer.listen(consumerRecord);

        verify(objectMapper).readValue(eq(jsonMessage), eq(WeatherInfo.class));
        verify(consumerRecord).value();
    }

    @Test
    void testListenWithInvalidJson() throws Exception {
        String invalidJson = "invalid json";
        when(consumerRecord.value()).thenReturn(invalidJson);
        when(objectMapper.readValue(invalidJson, WeatherInfo.class)).thenThrow(new RuntimeException("Invalid JSON"));

        assertThatThrownBy(() -> weatherConsumer.listen(consumerRecord)).isInstanceOf(Exception.class);
        verify(objectMapper).readValue(eq(invalidJson), eq(WeatherInfo.class));
    }

    @Test
    void testListenWithNullMessage() throws Exception {
        when(consumerRecord.value()).thenReturn(null);
        when(objectMapper.readValue((String) null, WeatherInfo.class)).thenReturn(null);

        weatherConsumer.listen(consumerRecord);

        verify(objectMapper).readValue((String) null, WeatherInfo.class);
    }

    @Test
    void testListenWithEmptyMessage() throws Exception {
        String emptyMessage = "";
        when(consumerRecord.value()).thenReturn(emptyMessage);
        when(objectMapper.readValue(emptyMessage, WeatherInfo.class)).thenThrow(new RuntimeException("Empty message"));

        assertThatThrownBy(() -> weatherConsumer.listen(consumerRecord)).isInstanceOf(Exception.class);
    }

    @Test
    void testListenWithValidWeatherData() throws Exception {
        String jsonMessage = """
                {
                    "city": "Санкт-Петербург",
                    "date": "2024-01-20",
                    "temperature": 0,
                    "condition": "снег"
                }""";
        WeatherInfo expectedWeatherInfo = new WeatherInfo(
                "Санкт-Петербург",
                LocalDate.of(2024, 1, 20),
                0,
                "снег");

        when(consumerRecord.value()).thenReturn(jsonMessage);
        when(objectMapper.readValue(jsonMessage, WeatherInfo.class)).thenReturn(expectedWeatherInfo);

        weatherConsumer.listen(consumerRecord);

        verify(objectMapper).readValue(eq(jsonMessage), eq(WeatherInfo.class));
    }

    @Test
    void testListenWithNegativeTemperature() throws Exception {
        String jsonMessage = """
                {
                    "city":"Новосибирск",
                    "date":"2024-01-25",
                    "temperature": 3,
                    "condition":"мороз"
                }""";
        WeatherInfo expectedWeatherInfo = new WeatherInfo(
                "Новосибирск",
                LocalDate.of(2024, 1, 25),
                3,
                "мороз");

        when(consumerRecord.value()).thenReturn(jsonMessage);
        when(objectMapper.readValue(jsonMessage, WeatherInfo.class)).thenReturn(expectedWeatherInfo);

        weatherConsumer.listen(consumerRecord);

        verify(objectMapper).readValue(eq(jsonMessage), eq(WeatherInfo.class));
    }

    @Test
    void testListenWithHighTemperature() throws Exception {
        String jsonMessage = """
                {
                    "city":"Сочи",
                    "date":"2024-01-30",
                    "temperature":35,
                    "condition":"жара"
                }""";
        WeatherInfo expectedWeatherInfo = new WeatherInfo(
                "Сочи",
                LocalDate.of(2024, 1, 30),
                35,
                "жара");

        when(consumerRecord.value()).thenReturn(jsonMessage);
        when(objectMapper.readValue(jsonMessage, WeatherInfo.class)).thenReturn(expectedWeatherInfo);

        weatherConsumer.listen(consumerRecord);

        verify(objectMapper).readValue(eq(jsonMessage), eq(WeatherInfo.class));
    }

    @Test
    void testListenWithObjectMapperException() throws Exception {
        String jsonMessage = """
                {
                    "city":"Тест"
                }""";
        when(consumerRecord.value()).thenReturn(jsonMessage);
        when(objectMapper.readValue(jsonMessage, WeatherInfo.class))
                .thenThrow(new RuntimeException("Deserialization failed"));

        assertThatThrownBy(() -> weatherConsumer.listen(consumerRecord)).isInstanceOf(Exception.class);
        verify(objectMapper).readValue(eq(jsonMessage), eq(WeatherInfo.class));
    }

    @Test
    void testListenWithSpecialCharacters() throws Exception {
        String jsonMessage = """
                {
                    "city":"Москва-Сити",
                    "date":"2024-01-15",
                    "temperature":10,
                    "condition":"дождь с градом"
                }""";
        WeatherInfo expectedWeatherInfo = new WeatherInfo(
                "Москва-Сити",
                LocalDate.of(2024, 1, 15),
                10,
                "дождь с градом");

        when(consumerRecord.value()).thenReturn(jsonMessage);
        when(objectMapper.readValue(jsonMessage, WeatherInfo.class)).thenReturn(expectedWeatherInfo);

        weatherConsumer.listen(consumerRecord);

        verify(objectMapper).readValue(eq(jsonMessage), eq(WeatherInfo.class));
    }
} 