package io.kgraph.rest.server;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ConfigurationProperties(prefix = "kafka.graphs")
public class KafkaGraphsProperties {
    private String bootstrapServers;
    private String zookeeperConnect;
}

