package io.kgraph.rest.server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.stereotype.Component;

@Component
public class AppContextEventListener {
    private static final Logger log = LoggerFactory.getLogger(AppContextEventListener.class);

    @EventListener
    public void handleContextRefreshed(ContextRefreshedEvent event) {
        logActiveProperties((ConfigurableEnvironment) event.getApplicationContext().getEnvironment());
    }

    private void logActiveProperties(ConfigurableEnvironment env) {
        List<MapPropertySource> propertySources = new ArrayList<>();

        for (PropertySource<?> source : env.getPropertySources()) {
            if (source instanceof MapPropertySource && source.getName().contains("applicationConfig")) {
                propertySources.add((MapPropertySource) source);
            }
        }

        propertySources.stream()
            .map(propertySource -> propertySource.getSource().keySet())
            .flatMap(Collection::stream)
            .distinct()
            .sorted()
            .forEach(key -> {
                try {
                    log.debug(key + "=" + env.getProperty(key));
                } catch (Exception e) {
                    log.warn("{} -> {}", key, e.getMessage());
                }
            });
    }
}
