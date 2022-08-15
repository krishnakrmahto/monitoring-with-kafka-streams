package com.sampleprojects.kafka.kafkastreams.stethoscope.processor;

import com.sampleprojects.kafka.kafkastreams.stethoscope.config.AppSerdes;
import com.sampleprojects.kafka.kafkastreams.stethoscope.config.clientinstanceeviction.ClientInstanceEvictionConfig;
import com.sampleprojects.kafka.kafkastreams.stethoscope.config.clientinstanceeviction.ClientInstanceEvictionInfo;
import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.consumed.Heartbeat;
import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.produced.ClientInstanceSet;
import com.sampleprojects.kafka.kafkastreams.stethoscope.dto.message.produced.DeadInstanceWindow;
import com.sampleprojects.kafka.kafkastreams.stethoscope.util.WindowedHeartbeats;
import java.util.Arrays;
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

public class DeadClientInstanceProcessorOneHourWindowWithNoGraceForTwoApplicationsTests {
  private TestInputTopic<String, Heartbeat> sourceTopic;

  private TestOutputTopic<DeadInstanceWindow, ClientInstanceSet> application1SinkTopic;
  private TestOutputTopic<DeadInstanceWindow, ClientInstanceSet> application2SinkTopic;

  private static final String heartbeatSourceTopic = "application.evaluateDeadInstance.heartbeat";

  private final static String application1Name = "application1";
  private final static String application2Name = "application2";

  private final static List<Heartbeat> firstWindowHeartbeats = WindowedHeartbeats.getFirstWindowHeartbeats();
  private final static List<Heartbeat> secondWindowHeartbeats = WindowedHeartbeats.getSecondWindowHeartbeats();
  private final static List<Heartbeat> thirdWindowHeartbeats = WindowedHeartbeats.getThirdWindowHeartbeats();
  private final static List<Heartbeat> fourthWindowHeartbeats = WindowedHeartbeats.getFourthWindowHeartbeats();

  @BeforeEach
  void beforeEach() {

    StreamsBuilder builder = new StreamsBuilder();
    HeartbeatTimestampExtractor timestampExtractor = new HeartbeatTimestampExtractor();

    ClientInstanceEvictionInfo application1Info = ClientInstanceEvictionInfo.builder()
        .applicationName(application1Name)
        .windowDurationSeconds(3600)
        .graceDurationSeconds(0)
        .sinkTopic("application1.deadInstances")
        .build();

    ClientInstanceEvictionInfo application2Info = ClientInstanceEvictionInfo.builder()
        .applicationName(application2Name)
        .windowDurationSeconds(3600)
        .graceDurationSeconds(0)
        .sinkTopic("application2.deadInstances")
        .build();

    List<ClientInstanceEvictionInfo> instanceEvictionInfos = Arrays.asList(application1Info, application2Info);

    ClientInstanceEvictionConfig instanceEvictionConfig = new ClientInstanceEvictionConfig(instanceEvictionInfos);

    DeadClientInstanceProcessor deadClientInstanceProcessor = new DeadClientInstanceProcessor(builder,
    timestampExtractor, instanceEvictionConfig);
    deadClientInstanceProcessor.addProcessingSteps();

    Topology topology = builder.build();
    @SuppressWarnings("resource")
    TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology);

    sourceTopic = topologyTestDriver.createInputTopic(heartbeatSourceTopic,
        Serdes.String().serializer(), AppSerdes.heartbeatSerde().serializer());

    String application1SinkTopicName = application1Info.getSinkTopic();
    String application2SinkTopicName = application2Info.getSinkTopic();
    application1SinkTopic = topologyTestDriver.createOutputTopic(application1SinkTopicName,
        AppSerdes.deadInstanceWindowSerde().deserializer(), AppSerdes.clientInstanceSetSerde().deserializer());
    application2SinkTopic = topologyTestDriver.createOutputTopic(application2SinkTopicName,
        AppSerdes.deadInstanceWindowSerde().deserializer(), AppSerdes.clientInstanceSetSerde().deserializer());
  }

  @Test
  void sinksEmptyDeadInstanceListWhenTheVeryFirstWindowClosesForBothApplications() {

    firstWindowHeartbeats.forEach(heartbeat -> sourceTopic.pipeInput(application1Name, heartbeat));
    firstWindowHeartbeats.forEach(heartbeat -> sourceTopic.pipeInput(application2Name, heartbeat));

    Heartbeat heartbeatThatClosesFirstWindow = secondWindowHeartbeats.get(0);
    sourceTopic.pipeInput(application1Name, heartbeatThatClosesFirstWindow);
    sourceTopic.pipeInput(application2Name, heartbeatThatClosesFirstWindow);

    Assertions.assertEquals(new ClientInstanceSet(Collections.emptySet()), application1SinkTopic.readValue());
    Assertions.assertEquals(new ClientInstanceSet(Collections.emptySet()), application2SinkTopic.readValue());
  }

  @Test
  void sinksEmptyDeadInstanceListForOnlyThatApplicationWhoseVeryFirstWindowCloses() {

    firstWindowHeartbeats.forEach(heartbeat -> sourceTopic.pipeInput(application1Name, heartbeat));
    firstWindowHeartbeats.forEach(heartbeat -> sourceTopic.pipeInput(application2Name, heartbeat));

    Heartbeat heartbeatThatClosesFirstWindow = secondWindowHeartbeats.get(0);
    sourceTopic.pipeInput(application1Name, heartbeatThatClosesFirstWindow);

    Assertions.assertEquals(new ClientInstanceSet(Collections.emptySet()), application1SinkTopic.readValue());
    Assertions.assertTrue(application2SinkTopic.isEmpty());
  }

  @Test
  void sinksDeadInstancesFromTheFirstWindowWhenTheSecondWindowClosesForBothApplications() {

    firstWindowHeartbeats.forEach(heartbeat -> sourceTopic.pipeInput(application1Name, heartbeat));
    firstWindowHeartbeats.forEach(heartbeat -> sourceTopic.pipeInput(application2Name, heartbeat));

    secondWindowHeartbeats.forEach(heartbeat -> sourceTopic.pipeInput(application1Name, heartbeat));
    secondWindowHeartbeats.forEach(heartbeat -> sourceTopic.pipeInput(application2Name, heartbeat));

    Assertions.assertEquals(new ClientInstanceSet(Collections.emptySet()), application1SinkTopic.readValue());
    Assertions.assertEquals(new ClientInstanceSet(Collections.emptySet()), application2SinkTopic.readValue());

    Heartbeat heartbeatThatClosesSecondWindow = thirdWindowHeartbeats.get(0);
    sourceTopic.pipeInput(application1Name, heartbeatThatClosesSecondWindow);
    sourceTopic.pipeInput(application2Name, heartbeatThatClosesSecondWindow);

    Assertions.assertEquals(new ClientInstanceSet(Collections.singleton("instance2")), application1SinkTopic.readValue());
    Assertions.assertEquals(new ClientInstanceSet(Collections.singleton("instance2")), application2SinkTopic.readValue());
  }

  @Test
  void sinksDeadInstancesFromTheFirstWindowForOnlyThatApplicationWhoseWindowCloses() {

    firstWindowHeartbeats.forEach(heartbeat -> sourceTopic.pipeInput(application1Name, heartbeat));
    firstWindowHeartbeats.forEach(heartbeat -> sourceTopic.pipeInput(application2Name, heartbeat));

    secondWindowHeartbeats.forEach(heartbeat -> sourceTopic.pipeInput(application1Name, heartbeat));
    secondWindowHeartbeats.forEach(heartbeat -> sourceTopic.pipeInput(application2Name, heartbeat));

    Assertions.assertEquals(new ClientInstanceSet(Collections.emptySet()), application1SinkTopic.readValue());
    Assertions.assertEquals(new ClientInstanceSet(Collections.emptySet()), application2SinkTopic.readValue());

    Heartbeat heartbeatThatClosesSecondWindow = thirdWindowHeartbeats.get(0);
    sourceTopic.pipeInput(application1Name, heartbeatThatClosesSecondWindow);

    Assertions.assertEquals(new ClientInstanceSet(Collections.singleton("instance2")), application1SinkTopic.readValue());
    Assertions.assertTrue(application2SinkTopic.isEmpty());
  }
}
