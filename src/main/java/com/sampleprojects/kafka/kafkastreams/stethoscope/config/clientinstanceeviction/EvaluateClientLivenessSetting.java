package com.sampleprojects.kafka.kafkastreams.stethoscope.config.clientinstanceeviction;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class EvaluateClientLivenessSetting {

  private String applicationName;
  private long windowDurationSeconds;
  private long graceDurationSeconds;
  private String sinkTopic;
}
