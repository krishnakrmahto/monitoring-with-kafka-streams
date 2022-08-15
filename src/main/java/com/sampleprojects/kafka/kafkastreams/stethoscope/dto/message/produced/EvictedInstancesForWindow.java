package com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.produced;

import lombok.Builder;

@Builder
public record EvictedInstancesForWindow(String applicationName, long startMs, long endMs) {

}
