package com.sampleprojects.kafka.kafkastreams.stethoscope.processor;

import com.sampleprojects.kafka.kafkastreams.stethoscope.config.AppSerdes;
import com.sampleprojects.kafka.kafkastreams.stethoscope.config.clientinstanceeviction.ClientInstanceEvictionConfig;
import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.produced.ClientInstanceSet;
import com.sampleprojects.kafka.kafkastreams.stethoscope.processor.statefultransformer.LastWindowDeadInstanceEvaluator;
import java.time.Duration;
import javax.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.Stores;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class DeadClientInstanceProcessor {

  private final StreamsBuilder builder;

  private final HeartbeatTimestampExtractor heartbeatTimestampExtractor;

  private final String stateStoreName = "test-state-store-11";

  private final ClientInstanceEvictionConfig clientInstanceEvictionConfig;

  private static final String heartbeatSourceTopic = "application.evaluateDeadInstance.heartbeat";

  @PostConstruct
  public void addProcessingSteps() {

    builder.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
        Serdes.String(), AppSerdes.clientInstanceSetSerde()));

    clientInstanceEvictionConfig.getInstanceEvictionInfo().forEach(clientInstanceEvictionInfo -> {

      TimeWindows timeWindows = getTimeWindows(clientInstanceEvictionInfo.getWindowDurationSeconds(),
          clientInstanceEvictionInfo.getGraceDurationSeconds());

      builder.stream(heartbeatSourceTopic,
              Consumed.with(Serdes.String(), AppSerdes.heartbeatSerde()).withTimestampExtractor(heartbeatTimestampExtractor))
          .filter(((key, value) -> key.equals(clientInstanceEvictionInfo.getApplicationName())))
          .groupByKey(Grouped.with(Serdes.String(), AppSerdes.heartbeatSerde()))
          .windowedBy(timeWindows)
          .aggregate(ClientInstanceSet::new, ((key, value, aggregate) -> aggregate.addInstance(value.getInstanceName())),
              Materialized.with(Serdes.String(), AppSerdes.clientInstanceSetSerde()))
          .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
          .toStream()
          .transform(() -> new LastWindowDeadInstanceEvaluator(stateStoreName), stateStoreName)
          .peek((key, evictedInstances) -> log.info("key: {}, value: {}", key, evictedInstances))
          .to(clientInstanceEvictionInfo.getSinkTopic(), Produced.with(AppSerdes.deadInstanceWindowSerde(),
              AppSerdes.clientInstanceSetSerde()));

    });
  }

  private TimeWindows getTimeWindows(long windowDurationSeconds, long graceDurationSeconds) {
    Duration windowDuration = Duration.ofSeconds(windowDurationSeconds);
    Duration graceDuration = Duration.ofSeconds(graceDurationSeconds);

    return graceDuration.isZero()? TimeWindows.ofSizeWithNoGrace(windowDuration):
        TimeWindows.ofSizeAndGrace(windowDuration, graceDuration);
  }

}
