package com.sampleprojects.kafka.kafkastreams.stethoscope.processor.statefultransformer;

import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.ClientInstanceSet;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

@Builder
@RequiredArgsConstructor
@AllArgsConstructor
public class EvictedInstanceReckoner implements Transformer<Windowed<String>, ClientInstanceSet, KeyValue<Windowed<String>, ClientInstanceSet>> {

  private final String stateStoreName;

  private KeyValueStore<String, ClientInstanceSet> stateStore;

  @Override
  public void init(ProcessorContext processorContext) {
    stateStore = processorContext.getStateStore(stateStoreName);
  }

  @Override
  public KeyValue<Windowed<String>, ClientInstanceSet> transform(Windowed<String> windowedKey, ClientInstanceSet latestHeartbeatSenders) {

    String applicationName = windowedKey.key();

    Optional<ClientInstanceSet> previousWindowInstances = Optional.ofNullable(stateStore.get(applicationName));

    if (previousWindowInstances.isPresent()) {
      ClientInstanceSet evictedInstances = previousWindowInstances.get().findEvictedInstances(latestHeartbeatSenders);

      stateStore.put(applicationName, latestHeartbeatSenders);

      return KeyValue.pair(windowedKey, evictedInstances);
    } else {
      stateStore.put(applicationName, latestHeartbeatSenders);

      return KeyValue.pair(windowedKey, new ClientInstanceSet());
    }
  }


  @Override
  public void close() {

  }
}

