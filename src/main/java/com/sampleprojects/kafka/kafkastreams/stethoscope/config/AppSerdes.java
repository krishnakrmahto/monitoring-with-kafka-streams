package com.sampleprojects.kafka.kafkastreams.stethoscope.config;

import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.ClientInstanceSet;
import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.Heartbeat;
import lombok.experimental.UtilityClass;
import org.apache.kafka.common.serialization.Serde;
import org.springframework.kafka.support.serializer.JsonSerde;

@UtilityClass
public class AppSerdes {

  public Serde<Heartbeat> heartbeatSerde() {
    return new JsonSerde<>(Heartbeat.class);
  }

  public Serde<ClientInstanceSet> clientInstanceSetSerde() {
    return new JsonSerde<>(ClientInstanceSet.class);
  }
}
