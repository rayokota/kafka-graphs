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

package io.kgraph.rest.server.graph;

import static org.springframework.web.reactive.function.server.RouterFunctions.route;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

@Configuration
public class GraphAlgorithmRouter {

    @Bean
    protected RouterFunction<ServerResponse> routingFunction(GraphAlgorithmHandler graphAlgorithmHandler) {
        return route(RequestPredicates.POST("/import"), graphAlgorithmHandler::importGraph)
            .andRoute(RequestPredicates.POST("/prepare"), graphAlgorithmHandler::prepareGraph)
            .andRoute(RequestPredicates.POST("/pregel"), graphAlgorithmHandler::configure)
            .andRoute(RequestPredicates.POST("/pregel/{id}"), graphAlgorithmHandler::run)
            .andRoute(RequestPredicates.GET("/pregel/{id}"), graphAlgorithmHandler::state)
            .andRoute(RequestPredicates.GET("/pregel/{id}/result"), graphAlgorithmHandler::result)
            .andRoute(RequestPredicates.DELETE("/pregel/{id}"), graphAlgorithmHandler::delete);
    }
}
