package com.sampleprojects.kafka.kafkastreams.stethoscope.config;

import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.ClientInstanceSet;
import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.Heartbeat;
import org.apache.kafka.common.serialization.Serde;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
public class SerdeBeanConfig {

  @Bean
  public Serde<Heartbeat> heartbeatSerde() {
    return new JsonSerde<>(Heartbeat.class);
  }

  @Bean
  public Serde<ClientInstanceSet> clientInstanceSetSerde() {
    return new JsonSerde<>(ClientInstanceSet.class);
  }
}
