package com.sampleprojects.kafka.kafkastreams.stethoscope.config.clientinstanceeviction;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class ClientInstanceEvictionInfo {

  private String applicationName;
  private long windowDurationSeconds;
  private long graceDurationSeconds;
  private String sinkTopic;
}
