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
