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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DeadClientInstanceProcessorOneHourWindowWithNoGraceForOneApplicationTests {

  private TestInputTopic<String, Heartbeat> sourceTopic;

  private TestOutputTopic<DeadInstanceWindow, ClientInstanceSet> applicationSinkTopic;

  private static final String heartbeatSourceTopic = "application.evaluateDeadInstance.heartbeat";

  private final static String applicationName = "application";

  private final static List<Heartbeat> firstWindowHeartbeats = WindowedHeartbeats.getFirstWindowHeartbeats();
  private final static List<Heartbeat> secondWindowHeartbeats = WindowedHeartbeats.getSecondWindowHeartbeats();
  private final static List<Heartbeat> thirdWindowHeartbeats = WindowedHeartbeats.getThirdWindowHeartbeats();
  private final static List<Heartbeat> fourthWindowHeartbeats = WindowedHeartbeats.getFourthWindowHeartbeats();

  @BeforeEach
  void setUp() {

    StreamsBuilder builder = new StreamsBuilder();
    HeartbeatTimestampExtractor timestampExtractor = new HeartbeatTimestampExtractor();

    ClientInstanceEvictionInfo testClientApplicationInfo1 = ClientInstanceEvictionInfo.builder()
        .applicationName(applicationName)
        .windowDurationSeconds(3600)
        .graceDurationSeconds(0)
        .sinkTopic("application.deadInstances")
        .build();

    List<ClientInstanceEvictionInfo> instanceEvictionInfos = Collections.singletonList(testClientApplicationInfo1);

    ClientInstanceEvictionConfig instanceEvictionConfig = new ClientInstanceEvictionConfig(
        instanceEvictionInfos);

    DeadClientInstanceProcessor deadClientInstanceProcessor = new DeadClientInstanceProcessor(builder,
        timestampExtractor, instanceEvictionConfig);
    deadClientInstanceProcessor.addProcessingSteps();

    Topology topology = builder.build();
    TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology);

    sourceTopic = topologyTestDriver.createInputTopic(heartbeatSourceTopic,
        Serdes.String().serializer(), AppSerdes.heartbeatSerde().serializer());

    String sinkTopic = instanceEvictionConfig.getInstanceEvictionInfo().get(0).getSinkTopic();
    applicationSinkTopic = topologyTestDriver.createOutputTopic(
        sinkTopic, AppSerdes.deadInstanceWindowSerde().deserializer(), AppSerdes.clientInstanceSetSerde().deserializer());
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
}
