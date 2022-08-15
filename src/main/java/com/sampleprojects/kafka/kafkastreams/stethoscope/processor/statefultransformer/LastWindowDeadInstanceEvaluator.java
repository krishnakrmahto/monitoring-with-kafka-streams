package com.sampleprojects.kafka.kafkastreams.stethoscope.processor.statefultransformer;

import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.produced.ClientInstanceSet;
import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.produced.DeadInstanceWindow;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

@RequiredArgsConstructor
public class LastWindowDeadInstanceEvaluator implements Transformer<Windowed<String>, ClientInstanceSet, KeyValue<DeadInstanceWindow, ClientInstanceSet>> {

  private final String stateStoreName;

  private KeyValueStore<String, ClientInstanceSet> clientAvailableInstances;

  @Override
  public void init(ProcessorContext processorContext) {
    clientAvailableInstances = processorContext.getStateStore(stateStoreName);
  }

  @Override
  public KeyValue<DeadInstanceWindow, ClientInstanceSet> transform(Windowed<String> windowedKey, ClientInstanceSet closedWindowClientInstances) {

    String applicationName = windowedKey.key();

    Optional<ClientInstanceSet> previousWindowInstances = Optional.ofNullable(
        clientAvailableInstances.get(applicationName));

    Window closedWindow = windowedKey.window();
    DeadInstanceWindow deadInstanceWindow = DeadInstanceWindow.builder()
        .applicationName(applicationName)
        .startMs(closedWindow.start())
        .endMs(closedWindow.end())
        .build();

    if (previousWindowInstances.isPresent()) {
      ClientInstanceSet evictedInstances = previousWindowInstances.get().findEvictedInstances(closedWindowClientInstances);

      clientAvailableInstances.put(applicationName, closedWindowClientInstances);

      return KeyValue.pair(deadInstanceWindow, evictedInstances);
    } else {
      clientAvailableInstances.put(applicationName, closedWindowClientInstances);

      return KeyValue.pair(deadInstanceWindow, new ClientInstanceSet());
    }
  }


  @Override
  public void close() {

  }
}

