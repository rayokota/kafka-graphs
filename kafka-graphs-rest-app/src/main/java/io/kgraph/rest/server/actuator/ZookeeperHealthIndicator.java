package io.kgraph.rest.server.actuator;

import org.apache.curator.framework.CuratorFramework;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.stereotype.Component;

import reactor.core.publisher.Mono;

@Component
public class ZookeeperHealthIndicator implements ReactiveHealthIndicator {

    private final CuratorFramework curator;

    public ZookeeperHealthIndicator(CuratorFramework curator) {
        this.curator = curator;
    }

    @Override
    public Mono<Health> health() {
        Health.Builder builder = new Health.Builder();
        if (curator.getZookeeperClient().isConnected()) {
            builder.up();
        } else {
            builder.down();
        }
        return Mono.just(builder.build());
    }
}