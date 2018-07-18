/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
