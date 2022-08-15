package com.sampleprojects.kafka.kafkastreams.stethoscope.processor;

import com.sampleprojects.kafka.kafkastreams.stethoscope.config.AppSerdes;
import com.sampleprojects.kafka.kafkastreams.stethoscope.config.clientinstanceeviction.ClientInstanceEvictionConfig;
import com.sampleprojects.kafka.kafkastreams.stethoscope.config.clientinstanceeviction.ClientInstanceEvictionInfo;
import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.consumed.Heartbeat;
import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.produced.ClientInstanceSet;
import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.produced.DeadInstanceWindow;
import java.util.Arrays;
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

class DeadClientInstanceProcessorOneHourWindowedWithNoGraceTests {

  private static TopologyTestDriver topologyTestDriver;

  private static TestInputTopic<String, Heartbeat> testSourceTopic;

  private static final String heartbeatSourceTopic = "application.evaluateDeadInstance.heartbeat";

  private final static String application1Name = "application1";

  private final static ClientInstanceEvictionInfo testClientApplicationInfo1 = ClientInstanceEvictionInfo.builder()
      .applicationName(application1Name)
      .windowDurationSeconds(3600)
      .graceDurationSeconds(0)
      .sinkTopic("application1.deadInstances")
      .build();
  private final static String application2Name = "application2";

  @BeforeAll
  static void setUp() {

    StreamsBuilder builder = new StreamsBuilder();
    HeartbeatTimestampExtractor timestampExtractor = new HeartbeatTimestampExtractor();

    ClientInstanceEvictionInfo testClientApplicationInfo2 = ClientInstanceEvictionInfo.builder()
        .applicationName(application2Name)
        .windowDurationSeconds(3600)
        .graceDurationSeconds(0)
        .sinkTopic("application2.deadInstances")
        .build();

    List<ClientInstanceEvictionInfo> instanceEvictionInfos = Arrays.asList(testClientApplicationInfo1, testClientApplicationInfo2);
    ClientInstanceEvictionConfig instanceEvictionConfig = new ClientInstanceEvictionConfig(instanceEvictionInfos);

    DeadClientInstanceProcessor deadClientInstanceProcessor = new DeadClientInstanceProcessor(builder,
        timestampExtractor, instanceEvictionConfig);
    deadClientInstanceProcessor.addProcessingSteps();

    Topology topology = builder.build();
    topologyTestDriver = new TopologyTestDriver(topology);

    testSourceTopic = topologyTestDriver.createInputTopic(heartbeatSourceTopic,
        Serdes.String().serializer(), AppSerdes.heartbeatSerde().serializer());
  }

  @Test
  void sinksEmptyDeadInstanceListWhenTheVeryFirstWindowCloses() {
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


    Stream.of(instance1Heartbeat, instance2Heartbeat, instance3Heartbeat)
        .forEach(testRecord -> testSourceTopic.pipeInput(application1Name, instance1Heartbeat));

    TestOutputTopic<DeadInstanceWindow, ClientInstanceSet> testSinkTopic = topologyTestDriver.createOutputTopic(testClientApplicationInfo1.getSinkTopic(),
        AppSerdes.deadInstanceWindowSerde().deserializer(), AppSerdes.clientInstanceSetSerde().deserializer());

    Heartbeat heartbeatThatClosesFirstWindow = Heartbeat.builder()
        .instanceName("instance1")
        .heartbeatEpoch(1660460001)
        .build();

    testSourceTopic.pipeInput(application1Name, heartbeatThatClosesFirstWindow);

    Assertions.assertEquals(testSinkTopic.readValue(), new ClientInstanceSet(Collections.emptySet()));
  }
}