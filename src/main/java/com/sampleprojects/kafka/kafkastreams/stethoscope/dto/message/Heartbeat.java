package com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Heartbeat {

  private String instanceName;

  private long heartbeatEpoch;

}
