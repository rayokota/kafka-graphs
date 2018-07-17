package io.kgraph.rest.server.actuator;

import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

import io.kgraph.rest.server.KafkaGraphsProperties;
import reactor.core.publisher.Mono;

@Component
@EnableConfigurationProperties(KafkaGraphsProperties.class)
public class KafkaHealthIndicator implements ReactiveHealthIndicator {

    private final KafkaGraphsProperties props;

    public KafkaHealthIndicator(KafkaGraphsProperties props) {
        this.props = props;
    }

    @Override
    public Mono<Health> health() {
        Health.Builder builder = new Health.Builder();
        Properties properties = new Properties();
        properties.put("bootstrap.servers", props.getBootstrapServers());
        try (AdminClient adminClient = AdminClient.create(properties)) {
            DescribeClusterResult result = adminClient.describeCluster(new DescribeClusterOptions().timeoutMs(3000));
            builder.withDetail("clusterId", result.clusterId().get());
            builder.up();
        } catch (Exception e) {
            builder.down();
        }
        return Mono.just(builder.build());
    }
}