package com.vay.h1.model;

import lombok.*;

import java.time.LocalDate;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class WeatherInfo {
    private String city;
    private LocalDate date;
    private Double temperature;
    private String condition;
} 