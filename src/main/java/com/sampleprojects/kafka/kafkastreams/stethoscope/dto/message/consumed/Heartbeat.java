package com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.consumed;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Heartbeat {

  private String instanceName;

  private long heartbeatEpoch;

}
