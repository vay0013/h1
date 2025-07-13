package com.vay.h1.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vay.h1.model.WeatherInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class WeatherProducerTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private ObjectMapper objectMapper;

    private WeatherProducer weatherProducer;

    @BeforeEach
    void setUp() {
        weatherProducer = new WeatherProducer(kafkaTemplate, objectMapper);
    }

    @Test
    void testSendWeatherSuccess() throws JsonProcessingException {
        String expectedJson = """
                {
                    "city":"Магадан",
                    "date":"2024-01-15",
                    "temperature":25,
                    "condition":"солнечно"
                }""";

        when(objectMapper.writeValueAsString(any(WeatherInfo.class))).thenReturn(expectedJson);
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        weatherProducer.sendWeather();

        verify(kafkaTemplate, times(4)).send(eq("weather"), anyString(), eq(expectedJson));
        verify(objectMapper, times(4)).writeValueAsString(any(WeatherInfo.class));
    }

    @Test
    void testSendWeatherWithJsonProcessingException() throws JsonProcessingException {
        when(objectMapper.writeValueAsString(any(WeatherInfo.class)))
                .thenThrow(new JsonProcessingException("Test exception") {
                });

        assertThatThrownBy(() -> weatherProducer.sendWeather())
                .isInstanceOf(JsonProcessingException.class);
    }

    @Test
    void testSendWeatherGeneratesValidWeatherInfo() throws JsonProcessingException {
        ArgumentCaptor<WeatherInfo> weatherInfoCaptor = ArgumentCaptor.forClass(WeatherInfo.class);
        when(objectMapper.writeValueAsString(any(WeatherInfo.class))).thenReturn("test");
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        weatherProducer.sendWeather();

        verify(objectMapper, times(4)).writeValueAsString(weatherInfoCaptor.capture());

        List<WeatherInfo> capturedWeatherInfos = weatherInfoCaptor.getAllValues();
        assertThat(capturedWeatherInfos).hasSize(4);

        List<String> expectedCities = List.of("Магадан", "Чукотка", "Питер", "Тюмень");
        List<String> actualCities = capturedWeatherInfos.stream()
                .map(WeatherInfo::getCity)
                .toList();

        assertThat(actualCities).containsExactlyInAnyOrderElementsOf(expectedCities);

        List<String> expectedConditions = List.of("солнечно", "облачно", "дождь");
        capturedWeatherInfos.forEach(weatherInfo -> {
            assertThat(weatherInfo.getCity()).isNotNull();
            assertThat(weatherInfo.getDate()).isNotNull();
            assertThat(weatherInfo.getTemperature()).isBetween(0, 35);
            assertThat(expectedConditions).contains(weatherInfo.getCondition());
        });
    }

    @Test
    void testSendWeatherUsesCorrectTopic() throws JsonProcessingException {
        when(objectMapper.writeValueAsString(any(WeatherInfo.class))).thenReturn("test");
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        weatherProducer.sendWeather();

        verify(kafkaTemplate, times(4)).send(eq("weather"), anyString(), anyString());
    }

    @Test
    void testSendWeatherUsesCityAsKey() throws JsonProcessingException {
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        when(objectMapper.writeValueAsString(any(WeatherInfo.class))).thenReturn("test");
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        weatherProducer.sendWeather();

        verify(kafkaTemplate, times(4)).send(anyString(), keyCaptor.capture(), anyString());

        List<String> capturedKeys = keyCaptor.getAllValues();
        List<String> expectedCities = List.of("Магадан", "Чукотка", "Питер", "Тюмень");

        assertThat(capturedKeys).containsExactlyInAnyOrderElementsOf(expectedCities);
    }

    @Test
    void testSendWeatherTemperatureRange() throws JsonProcessingException {
        ArgumentCaptor<WeatherInfo> weatherInfoCaptor = ArgumentCaptor.forClass(WeatherInfo.class);
        when(objectMapper.writeValueAsString(any(WeatherInfo.class))).thenReturn("test");
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        weatherProducer.sendWeather();

        verify(objectMapper, times(4)).writeValueAsString(weatherInfoCaptor.capture());

        weatherInfoCaptor.getAllValues().forEach(weatherInfo -> {
            assertThat(weatherInfo.getTemperature()).isGreaterThanOrEqualTo(0);
            assertThat(weatherInfo.getTemperature()).isLessThan(36);
        });
    }

    @Test
    void testSendWeatherDateRange() throws JsonProcessingException {
        ArgumentCaptor<WeatherInfo> weatherInfoCaptor = ArgumentCaptor.forClass(WeatherInfo.class);
        when(objectMapper.writeValueAsString(any(WeatherInfo.class))).thenReturn("test");
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        LocalDate today = LocalDate.now();

        weatherProducer.sendWeather();

        verify(objectMapper, times(4)).writeValueAsString(weatherInfoCaptor.capture());

        weatherInfoCaptor.getAllValues().forEach(weatherInfo -> {
            assertThat(weatherInfo.getDate()).isBefore(today.plusDays(1));
            assertThat(weatherInfo.getDate()).isAfter(today.minusDays(8));
        });
    }
} 