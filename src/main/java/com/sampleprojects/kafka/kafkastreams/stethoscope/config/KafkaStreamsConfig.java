package com.sampleprojects.kafka.kafkastreams.stethoscope.config;

import com.sampleprojects.kafka.kafkastreams.stethoscope.config.clientinstanceeviction.EvaluateClientLivenessConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableConfigurationProperties(EvaluateClientLivenessConfig.class)
@EnableKafkaStreams
@RequiredArgsConstructor
public class KafkaStreamsConfig {
}
