package com.datastream.mvp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class MvpBackendApplication {
    public static void main(String[] args) {
        SpringApplication.run(MvpBackendApplication.class, args);
    }
}


