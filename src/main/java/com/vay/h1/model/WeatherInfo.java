package com.vay.h1.model;

import lombok.*;

import java.time.LocalDate;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class WeatherInfo {
    private String city;
    private LocalDate date;
    private int temperature;
    private String condition;
} 