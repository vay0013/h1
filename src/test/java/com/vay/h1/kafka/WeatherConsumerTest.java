package com.vay.h1.kafka;

import com.vay.h1.model.WeatherInfo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.LocalDate;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class WeatherConsumerTest {

    @Mock
    private ConsumerRecord<String, WeatherInfo> consumerRecord;

    private WeatherConsumer weatherConsumer;

    @BeforeEach
    void setUp() {
        weatherConsumer = new WeatherConsumer();
    }

    @Test
    void testListenWithValidWeatherInfo() {
        WeatherInfo info = new WeatherInfo("Москва", LocalDate.of(2024, 1, 15), 25.0, "солнечно");
        when(consumerRecord.key()).thenReturn("Москва");
        when(consumerRecord.value()).thenReturn(info);

        weatherConsumer.listener(consumerRecord);

        verify(consumerRecord).key();
        verify(consumerRecord).value();
    }

    @Test
    void testListenWithNullWeatherInfo() {
        when(consumerRecord.key()).thenReturn("Москва");
        when(consumerRecord.value()).thenReturn(null);

        weatherConsumer.listener(consumerRecord);

        verify(consumerRecord).key();
        verify(consumerRecord).value();
    }

    @Test
    void testListenWithDifferentCities() {
        WeatherInfo info1 = new WeatherInfo("Питер", LocalDate.of(2024, 1, 20), 0.0, "снег");
        WeatherInfo info2 = new WeatherInfo("Сочи", LocalDate.of(2024, 1, 30), 35.0, "жара");
        when(consumerRecord.key()).thenReturn("Питер").thenReturn("Сочи");
        when(consumerRecord.value()).thenReturn(info1).thenReturn(info2);

        weatherConsumer.listener(consumerRecord);
        weatherConsumer.listener(consumerRecord);

        verify(consumerRecord, times(2)).key();
        verify(consumerRecord, times(2)).value();
    }
}