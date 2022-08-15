package com.sampleprojects.kafka.kafkastreams.stethoscope.processor.statefultransformer;

import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.ClientInstanceSet;
import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.produced.EvictedInstancesForWindow;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

@Builder
@RequiredArgsConstructor
@AllArgsConstructor
public class LastWindowDeadInstanceEvaluator implements Transformer<Windowed<String>, ClientInstanceSet, KeyValue<EvictedInstancesForWindow, ClientInstanceSet>> {

  private final String stateStoreName;

  private KeyValueStore<String, ClientInstanceSet> clientAvailableInstances;

  @Override
  public void init(ProcessorContext processorContext) {
    clientAvailableInstances = processorContext.getStateStore(stateStoreName);
  }

  @Override
  public KeyValue<EvictedInstancesForWindow, ClientInstanceSet> transform(Windowed<String> windowedKey, ClientInstanceSet closedWindowClientInstances) {

    String applicationName = windowedKey.key();

    Optional<ClientInstanceSet> previousWindowInstances = Optional.ofNullable(
        clientAvailableInstances.get(applicationName));

    Window closedWindow = windowedKey.window();
    EvictedInstancesForWindow evictedInstancesForWindow = EvictedInstancesForWindow.builder()
        .applicationName(applicationName)
        .startMs(closedWindow.start())
        .endMs(closedWindow.end())
        .build();

    if (previousWindowInstances.isPresent()) {
      ClientInstanceSet evictedInstances = previousWindowInstances.get().findEvictedInstances(closedWindowClientInstances);

      clientAvailableInstances.put(applicationName, closedWindowClientInstances);

      return KeyValue.pair(evictedInstancesForWindow, evictedInstances);
    } else {
      clientAvailableInstances.put(applicationName, closedWindowClientInstances);

      return KeyValue.pair(evictedInstancesForWindow, new ClientInstanceSet());
    }
  }


  @Override
  public void close() {

  }
}

