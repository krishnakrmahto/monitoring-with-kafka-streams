package com.sampleprojects.kafka.kafkastreams.stethoscope.processor;

import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.ClientInstanceSet;
import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.Heartbeat;
import com.sampleprojects.kafka.kafkastreams.stethoscope.processor.statefultransformer.EvictedInstanceReckoner;
import java.time.Duration;
import javax.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.Stores;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class HeartbeatProcessor {

  private final StreamsBuilder builder;

  private final Serde<Heartbeat> heartbeatSerde;

  private final Serde<ClientInstanceSet> heartbeatSenderInstancesSerde;

  private final HeartbeatTimestampExtractor heartbeatTimestampExtractor;

  private final String stateStoreName = "clientAvailableInstances";

  @PostConstruct
  public void addProcessingSteps() {

    Duration windowSize = Duration.ofHours(1);

    KStream<String, Heartbeat> sourceStream = builder.stream("application.heartbeat",
        Consumed.with(Serdes.String(), heartbeatSerde).withTimestampExtractor(
            heartbeatTimestampExtractor));

    builder.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
        Serdes.String(), heartbeatSenderInstancesSerde));

    sourceStream
        .groupByKey(Grouped.with(Serdes.String(), heartbeatSerde))
        .windowedBy(TimeWindows.ofSizeWithNoGrace(windowSize))
        .aggregate(ClientInstanceSet::new, ((key, value, aggregate) -> aggregate.addInstance(value.getInstanceName())),
            Materialized.with(Serdes.String(), heartbeatSenderInstancesSerde))
        .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
        .toStream()
        .transform(() -> new EvictedInstanceReckoner(stateStoreName), stateStoreName)
        .foreach((windowedKey, value) -> log.info("Windowed start: {}; end: {}, key:{}, value: {}", windowedKey.window().start(),
            windowedKey.window().end(), windowedKey.key(), value));
  }

}
