package com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.produced;

import java.util.HashSet;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClientInstanceSet {

  private Set<String> instanceNames = new HashSet<>();

  public ClientInstanceSet addInstance(String instanceName) {
    instanceNames.add(instanceName);
    return this;
  }

  public ClientInstanceSet findEvictedInstances(ClientInstanceSet latestSenderInstances) {

    Set<String> evictedInstanceNames = new HashSet<>(instanceNames);
    evictedInstanceNames.removeAll(latestSenderInstances.getInstanceNames());

    return new ClientInstanceSet(evictedInstanceNames);

  }
}