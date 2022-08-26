package com.sampleprojects.kafka.kafkastreams.stethoscope.processor;

import com.sampleprojects.kafka.kafkastreams.stethoscope.config.AppSerdes;
import com.sampleprojects.kafka.kafkastreams.stethoscope.config.clientinstanceeviction.ClientInstanceEvictionConfig;
import com.sampleprojects.kafka.kafkastreams.stethoscope.config.clientinstanceeviction.ClientInstanceEvictionInfo;
import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.consumed.Heartbeat;
import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.produced.ClientInstanceSet;
import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.produced.DeadInstanceWindow;
import com.sampleprojects.kafka.kafkastreams.stethoscope.util.WindowedHeartbeats;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DeadClientInstanceProcessorOneHourWindowNoGraceOneClientTests {

  private TestInputTopic<String, Heartbeat> sourceTopic;

  private TestOutputTopic<DeadInstanceWindow, ClientInstanceSet> applicationSinkTopic;

  private TopologyTestDriver topologyTestDriver;

  private static final String evaluateDeadInstanceSourceTopic = "application.evaluateDeadInstance.heartbeat";

  private final static String applicationName = "application";

  private final static List<Heartbeat> firstWindowHeartbeats = WindowedHeartbeats.getFirstWindowHeartbeats();
  private final static List<Heartbeat> secondWindowHeartbeats = WindowedHeartbeats.getSecondWindowHeartbeats();
  private final static List<Heartbeat> thirdWindowHeartbeats = WindowedHeartbeats.getThirdWindowHeartbeats();
  private final static List<Heartbeat> fourthWindowHeartbeats = WindowedHeartbeats.getFourthWindowHeartbeats();

  @BeforeEach
  void beforeEach() {

    StreamsBuilder builder = new StreamsBuilder();
    HeartbeatTimestampExtractor timestampExtractor = new HeartbeatTimestampExtractor();

    ClientInstanceEvictionInfo testClientApplicationInfo = ClientInstanceEvictionInfo.builder()
        .applicationName(applicationName)
        .windowDurationSeconds(3600)
        .graceDurationSeconds(0)
        .sinkTopic("application.deadInstances")
        .build();

    List<ClientInstanceEvictionInfo> instanceEvictionInfos = Collections.singletonList(testClientApplicationInfo);

    ClientInstanceEvictionConfig instanceEvictionConfig = new ClientInstanceEvictionConfig(instanceEvictionInfos);

    DeadClientInstanceProcessor deadClientInstanceProcessor = new DeadClientInstanceProcessor(builder,
        timestampExtractor, instanceEvictionConfig);
    deadClientInstanceProcessor.processingSteps();

    Topology topology = builder.build();

    topologyTestDriver = new TopologyTestDriver(topology);

    sourceTopic = topologyTestDriver.createInputTopic(evaluateDeadInstanceSourceTopic,
        Serdes.String().serializer(), AppSerdes.heartbeatSerde().serializer());

    String sinkTopicName = testClientApplicationInfo.getSinkTopic();
    applicationSinkTopic = topologyTestDriver.createOutputTopic(sinkTopicName,
        AppSerdes.deadInstanceWindowSerde().deserializer(), AppSerdes.clientInstanceSetSerde().deserializer());
  }

  @AfterEach
  void afterEach() {
    topologyTestDriver.close();
  }

  @Test
  void sinksEmptyDeadInstanceListWhenTheVeryFirstWindowCloses() {

    firstWindowHeartbeats.forEach(heartbeat -> sourceTopic.pipeInput(applicationName, heartbeat));

    Heartbeat heartbeatThatClosesFirstWindow = secondWindowHeartbeats.get(0);
    sourceTopic.pipeInput(applicationName, heartbeatThatClosesFirstWindow);

    Assertions.assertEquals(new ClientInstanceSet(Collections.emptySet()), applicationSinkTopic.readValue());
  }

  @Test
  void sinksDeadInstancesFromTheFirstWindowWhenTheSecondWindowCloses() {

    firstWindowHeartbeats.forEach(heartbeat -> sourceTopic.pipeInput(applicationName, heartbeat));

    secondWindowHeartbeats.forEach(heartbeat -> sourceTopic.pipeInput(applicationName, heartbeat));

    Assertions.assertEquals(new ClientInstanceSet(Collections.emptySet()), applicationSinkTopic.readValue());

    Heartbeat heartbeatThatClosesSecondWindow = thirdWindowHeartbeats.get(0);
    sourceTopic.pipeInput(applicationName, heartbeatThatClosesSecondWindow);

    Assertions.assertEquals(new ClientInstanceSet(Collections.singleton("instance2")), applicationSinkTopic.readValue());
  }

  @Test
  void sinksDeadInstancesFromTheSecondWindowWhenTheThirdWindowCloses() {

    firstWindowHeartbeats.forEach(heartbeat -> sourceTopic.pipeInput(applicationName, heartbeat));

    secondWindowHeartbeats.forEach(heartbeat -> sourceTopic.pipeInput(applicationName, heartbeat));

    Assertions.assertEquals(new ClientInstanceSet(Collections.emptySet()), applicationSinkTopic.readValue());

    thirdWindowHeartbeats.forEach(heartbeat -> sourceTopic.pipeInput(applicationName, heartbeat));
    Assertions.assertEquals(new ClientInstanceSet(Collections.singleton("instance2")), applicationSinkTopic.readValue());

    fourthWindowHeartbeats.forEach(heartbeat -> sourceTopic.pipeInput(applicationName, heartbeat));
    Assertions.assertEquals(new ClientInstanceSet(Stream.of("instance4", "instance5").collect(
        Collectors.toSet())), applicationSinkTopic.readValue());
  }
}
