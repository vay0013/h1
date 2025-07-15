package com.vay.h1.kafka;

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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class WeatherProducerTest {

    @Mock
    private KafkaTemplate<String, WeatherInfo> kafkaTemplate;

    private WeatherProducer weatherProducer;

    @BeforeEach
    void setUp() {
        weatherProducer = new WeatherProducer(kafkaTemplate);
    }

    @Test
    void testSendWeatherSuccess() {
        when(kafkaTemplate.send(anyString(), anyString(), any(WeatherInfo.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        weatherProducer.sendWeather();

        verify(kafkaTemplate, atLeastOnce()).send(eq("weather"), anyString(), any(WeatherInfo.class));
    }

    @Test
    void testSendWeatherGeneratesValidWeatherInfo() {
        ArgumentCaptor<WeatherInfo> weatherInfoCaptor = ArgumentCaptor.forClass(WeatherInfo.class);
        when(kafkaTemplate.send(anyString(), anyString(), any(WeatherInfo.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        weatherProducer.sendWeather();

        verify(kafkaTemplate, atLeastOnce()).send(anyString(), anyString(), weatherInfoCaptor.capture());

        List<WeatherInfo> capturedWeatherInfos = weatherInfoCaptor.getAllValues();
        assertThat(capturedWeatherInfos).isNotEmpty();
        capturedWeatherInfos.forEach(weatherInfo -> {
            assertThat(weatherInfo.getCity()).isNotNull();
            assertThat(weatherInfo.getDate()).isNotNull();
            assertThat(weatherInfo.getTemperature()).isBetween(0.0, 36.0);
            assertThat(weatherInfo.getCondition()).isNotNull();
        });
    }

    @Test
    void testSendWeatherUsesCorrectTopic() {
        when(kafkaTemplate.send(anyString(), anyString(), any(WeatherInfo.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        weatherProducer.sendWeather();

        verify(kafkaTemplate, atLeastOnce()).send(eq("weather"), anyString(), any(WeatherInfo.class));
    }

    @Test
    void testSendWeatherUsesCityAsKey() {
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        when(kafkaTemplate.send(anyString(), anyString(), any(WeatherInfo.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        weatherProducer.sendWeather();

        verify(kafkaTemplate, atLeastOnce()).send(anyString(), keyCaptor.capture(), any(WeatherInfo.class));

        List<String> capturedKeys = keyCaptor.getAllValues();
        assertThat(capturedKeys).isNotEmpty();
    }

    @Test
    void testSendWeatherTemperatureRange() {
        ArgumentCaptor<WeatherInfo> weatherInfoCaptor = ArgumentCaptor.forClass(WeatherInfo.class);
        when(kafkaTemplate.send(anyString(), anyString(), any(WeatherInfo.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        weatherProducer.sendWeather();

        verify(kafkaTemplate, atLeastOnce()).send(anyString(), anyString(), weatherInfoCaptor.capture());

        weatherInfoCaptor.getAllValues().forEach(weatherInfo -> {
            assertThat(weatherInfo.getTemperature()).isGreaterThanOrEqualTo(0.0);
            assertThat(weatherInfo.getTemperature()).isLessThanOrEqualTo(36.0);
        });
    }

    @Test
    void testSendWeatherDateRange() {
        ArgumentCaptor<WeatherInfo> weatherInfoCaptor = ArgumentCaptor.forClass(WeatherInfo.class);
        when(kafkaTemplate.send(anyString(), anyString(), any(WeatherInfo.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        LocalDate today = LocalDate.now();

        weatherProducer.sendWeather();

        verify(kafkaTemplate, atLeastOnce()).send(anyString(), anyString(), weatherInfoCaptor.capture());

        weatherInfoCaptor.getAllValues().forEach(weatherInfo -> {
            assertThat(weatherInfo.getDate()).isBefore(today.plusDays(1));
            assertThat(weatherInfo.getDate()).isAfter(today.minusDays(8));
        });
    }
}