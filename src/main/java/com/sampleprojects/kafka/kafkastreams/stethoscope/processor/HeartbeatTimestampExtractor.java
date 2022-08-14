package com.sampleprojects.kafka.kafkastreams.stethoscope.processor;

import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.Heartbeat;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.springframework.stereotype.Component;

@Component
public class HeartbeatTimestampExtractor implements TimestampExtractor {

  @Override
  public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
    Heartbeat event = (Heartbeat) record.value();
    return event.getHeartbeatEpoch() * 100;
  }
}
