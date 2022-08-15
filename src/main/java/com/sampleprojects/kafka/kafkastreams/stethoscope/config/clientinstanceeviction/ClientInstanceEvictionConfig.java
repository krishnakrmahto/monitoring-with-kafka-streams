package com.sampleprojects.kafka.kafkastreams.stethoscope.config.clientinstanceeviction;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "stethoscope.clients")
@NoArgsConstructor
@AllArgsConstructor
@Data
public class ClientInstanceEvictionConfig {

  List<ClientInstanceEvictionInfo> instanceEvictionInfo;

}
