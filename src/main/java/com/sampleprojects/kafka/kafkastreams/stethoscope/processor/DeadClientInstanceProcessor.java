package com.sampleprojects.kafka.kafkastreams.stethoscope.processor;

import com.sampleprojects.kafka.kafkastreams.stethoscope.config.AppSerdes;
import com.sampleprojects.kafka.kafkastreams.stethoscope.config.clientinstanceeviction.EvaluateClientLivenessConfig;
import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.consumed.Heartbeat;
import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.produced.ClientInstanceSet;
import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.produced.DeadInstanceWindow;
import com.sampleprojects.kafka.kafkastreams.stethoscope.processor.statefultransformer.LastWindowDeadInstanceEvaluator;
import java.time.Duration;
import javax.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
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
  private final EvaluateClientLivenessConfig evaluateClientLivenessConfig;

  private final String stateStoreName = "test-state-store-11";
  private static final String heartbeatSourceTopic = "application.evaluateDeadInstance.heartbeat";

  private static final Serde<String> stringSerde = Serdes.String();

  private static final Serde<ClientInstanceSet> clientInstanceSetSerde = AppSerdes.clientInstanceSetSerde();

  private static final Serde<Heartbeat> heartbeatSerde = AppSerdes.heartbeatSerde();

  private static final Serde<DeadInstanceWindow> deadInstanceWindowSerde = AppSerdes.deadInstanceWindowSerde();

  @PostConstruct
  public void processingSteps() {

    builder.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
        stringSerde, clientInstanceSetSerde));

    evaluateClientLivenessConfig.getEvaluateClientLivenessSettings().forEach(clientInstanceEvictionInfo -> {

      TimeWindows timeWindows = getTimeWindows(clientInstanceEvictionInfo.getWindowDurationSeconds(),
          clientInstanceEvictionInfo.getGraceDurationSeconds());

      builder.stream(heartbeatSourceTopic,
              Consumed.with(stringSerde, heartbeatSerde).withTimestampExtractor(heartbeatTimestampExtractor))
          .filter(((key, value) -> key.equals(clientInstanceEvictionInfo.getApplicationName())))
          .groupByKey(Grouped.with(stringSerde, heartbeatSerde))
          .windowedBy(timeWindows)
          .aggregate(ClientInstanceSet::new, ((key, value, aggregate) -> aggregate.addInstance(value.getInstanceName())),
              Materialized.with(stringSerde, clientInstanceSetSerde))
          .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
          .toStream()
          .transform(() -> new LastWindowDeadInstanceEvaluator(stateStoreName), stateStoreName)
          .peek((key, evictedInstances) -> log.info("key: {}, value: {}", key, evictedInstances))
          .to(clientInstanceEvictionInfo.getSinkTopic(), Produced.with(deadInstanceWindowSerde, clientInstanceSetSerde));

    });
  }

  private TimeWindows getTimeWindows(long windowDurationSeconds, long graceDurationSeconds) {
    Duration windowDuration = Duration.ofSeconds(windowDurationSeconds);
    Duration graceDuration = Duration.ofSeconds(graceDurationSeconds);

    return graceDuration.isZero()? TimeWindows.ofSizeWithNoGrace(windowDuration):
        TimeWindows.ofSizeAndGrace(windowDuration, graceDuration);
  }

}
