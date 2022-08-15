package com.sampleprojects.kafka.kafkastreams.stethoscope.processor;

import com.sampleprojects.kafka.kafkastreams.stethoscope.config.AppSerdes;
import com.sampleprojects.kafka.kafkastreams.stethoscope.config.clientinstanceeviction.ClientInstanceEvictionConfig;
import com.sampleprojects.kafka.kafkastreams.stethoscope.config.clientinstanceeviction.ClientInstanceEvictionInfo;
import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.consumed.Heartbeat;
import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.produced.ClientInstanceSet;
import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.produced.DeadInstanceWindow;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DeadClientInstanceProcessorOneHourWindowWithNoGraceForOneApplicationTests {

  private static TopologyTestDriver topologyTestDriver;

  private static TestInputTopic<String, Heartbeat> sourceTopic;

  private static ClientInstanceEvictionConfig instanceEvictionConfig;

  private static final String heartbeatSourceTopic = "application.evaluateDeadInstance.heartbeat";

  private final static String applicationName = "application";

  @BeforeAll
  static void setUp() {

    StreamsBuilder builder = new StreamsBuilder();
    HeartbeatTimestampExtractor timestampExtractor = new HeartbeatTimestampExtractor();

    ClientInstanceEvictionInfo testClientApplicationInfo1 = ClientInstanceEvictionInfo.builder()
        .applicationName(applicationName)
        .windowDurationSeconds(3600)
        .graceDurationSeconds(0)
        .sinkTopic("application.deadInstances")
        .build();

    List<ClientInstanceEvictionInfo> instanceEvictionInfos = Collections.singletonList(testClientApplicationInfo1);

    instanceEvictionConfig = new ClientInstanceEvictionConfig(instanceEvictionInfos);

    DeadClientInstanceProcessor deadClientInstanceProcessor = new DeadClientInstanceProcessor(builder,
        timestampExtractor, instanceEvictionConfig);
    deadClientInstanceProcessor.addProcessingSteps();

    Topology topology = builder.build();
    topologyTestDriver = new TopologyTestDriver(topology);

    sourceTopic = topologyTestDriver.createInputTopic(heartbeatSourceTopic,
        Serdes.String().serializer(), AppSerdes.heartbeatSerde().serializer());
  }

  @Test
  void sinksEmptyDeadInstanceListWhenTheVeryFirstWindowCloses() {

    applicationSameWindowHeartbeatStream().forEach(heartbeat -> sourceTopic.pipeInput(applicationName, heartbeat));

    sourceTopic.pipeInput(applicationName, heartbeatThatClosesTheLastWindow());

    String sinkTopic = instanceEvictionConfig.getInstanceEvictionInfo().get(0).getSinkTopic();
    TestOutputTopic<DeadInstanceWindow, ClientInstanceSet> applicationSinkTopic = topologyTestDriver.createOutputTopic(
        sinkTopic, AppSerdes.deadInstanceWindowSerde().deserializer(), AppSerdes.clientInstanceSetSerde().deserializer());

    Assertions.assertEquals(applicationSinkTopic.readValue(), new ClientInstanceSet(Collections.emptySet()));
  }

  private Stream<Heartbeat> applicationSameWindowHeartbeatStream() {
    Heartbeat instance1Heartbeat = Heartbeat.builder()
        .instanceName("instance1")
        .heartbeatEpoch(1660455801)
        .build();

    Heartbeat instance2Heartbeat = Heartbeat.builder()
        .instanceName("instance2")
        .heartbeatEpoch(1660455802)
        .build();

    Heartbeat instance3Heartbeat = Heartbeat.builder()
        .instanceName("instance3")
        .heartbeatEpoch(1660455803)
        .build();

    return Stream.of(instance1Heartbeat, instance2Heartbeat, instance3Heartbeat);
  }

  private Heartbeat heartbeatThatClosesTheLastWindow() {
    return Heartbeat.builder()
        .instanceName("instance1")
        .heartbeatEpoch(1660460001)
        .build();
  }
}
