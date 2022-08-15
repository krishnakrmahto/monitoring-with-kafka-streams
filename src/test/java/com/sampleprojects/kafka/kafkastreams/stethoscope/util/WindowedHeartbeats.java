package com.sampleprojects.kafka.kafkastreams.stethoscope.util;

import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.consumed.Heartbeat;
import java.util.Arrays;
import java.util.List;
import lombok.experimental.UtilityClass;

@UtilityClass
public class WindowedHeartbeats {

  public List<Heartbeat> getFirstWindowHeartbeats() {
    Heartbeat instance1Heartbeat = Heartbeat.builder()
        .instanceName("instance1")
        .heartbeatEpoch(1660455801)
        .build();

    Heartbeat instance2Heartbeat = Heartbeat.builder()
        .instanceName("instance2")
        .heartbeatEpoch(1660455802)
        .build();

    Heartbeat instance3Heartbeat = Heartbeat.builder()
        .instanceName("instance3")
        .heartbeatEpoch(1660455803)
        .build();

    Heartbeat instance4Heartbeat = Heartbeat.builder()
        .instanceName("instance4")
        .heartbeatEpoch(1660455804)
        .build();

    Heartbeat instance5Heartbeat = Heartbeat.builder()
        .instanceName("instance5")
        .heartbeatEpoch(1660455805)
        .build();

    return Arrays.asList(instance1Heartbeat, instance2Heartbeat, instance3Heartbeat, instance4Heartbeat, instance5Heartbeat);
  }

  public List<Heartbeat> getSecondWindowHeartbeats() {
    Heartbeat instance1Heartbeat = Heartbeat.builder()
        .instanceName("instance1")
        .heartbeatEpoch(1660460001)
        .build();

    Heartbeat instance3Heartbeat = Heartbeat.builder()
        .instanceName("instance3")
        .heartbeatEpoch(1660460002)
        .build();

    Heartbeat instance4Heartbeat = Heartbeat.builder()
        .instanceName("instance4")
        .heartbeatEpoch(1660460003)
        .build();

    Heartbeat instance5Heartbeat = Heartbeat.builder()
        .instanceName("instance5")
        .heartbeatEpoch(1660460004)
        .build();

    return Arrays.asList(instance1Heartbeat, instance3Heartbeat, instance4Heartbeat, instance5Heartbeat);
  }

  public List<Heartbeat> getThirdWindowHeartbeats() {
    Heartbeat instance1Heartbeat = Heartbeat.builder()
        .instanceName("instance1")
        .heartbeatEpoch(1660464201)
        .build();

    Heartbeat instance2Heartbeat = Heartbeat.builder()
        .instanceName("instance2")
        .heartbeatEpoch(1660464202)
        .build();

    Heartbeat instance3Heartbeat = Heartbeat.builder()
        .instanceName("instance3")
        .heartbeatEpoch(1660464203)
        .build();

    return Arrays.asList(instance1Heartbeat, instance2Heartbeat, instance3Heartbeat);
  }

  public List<Heartbeat> getFourthWindowHeartbeats() {
    Heartbeat instance1Heartbeat = Heartbeat.builder()
        .instanceName("instance1")
        .heartbeatEpoch(1660468401)
        .build();

    Heartbeat instance2Heartbeat = Heartbeat.builder()
        .instanceName("instance2")
        .heartbeatEpoch(1660468402)
        .build();

    Heartbeat instance3Heartbeat = Heartbeat.builder()
        .instanceName("instance3")
        .heartbeatEpoch(1660468403)
        .build();

    Heartbeat instance4Heartbeat = Heartbeat.builder()
        .instanceName("instance4")
        .heartbeatEpoch(1660468404)
        .build();

    return Arrays.asList(instance1Heartbeat, instance2Heartbeat, instance3Heartbeat, instance4Heartbeat);
  }
}