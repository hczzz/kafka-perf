package com.example.kakfaclienttest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * @author huangchen
 * @date 2020-02-10 14:24
 */
//@Configuration
public class SpringKafkaConfig {
    private Logger logger = LoggerFactory.getLogger(SpringKafkaConfig.class);

    @KafkaListener(topics = "tmp3")
    public void listen1(String message) {
    }
}
