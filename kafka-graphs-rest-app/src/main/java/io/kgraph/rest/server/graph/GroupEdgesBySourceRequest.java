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

import io.kgraph.library.GraphAlgorithmType;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
class GroupEdgesBySourceRequest {
    private GraphAlgorithmType algorithm;
    private String initialVerticesTopic = null;
    private String initialEdgesTopic;
    private String verticesTopic;
    private String edgesGroupedBySourceTopic;
    private int numPartitions = 50;
    private short replicationFactor = 1;
    private boolean valuesOfTypeDouble = false;
    private boolean async = true;
}
