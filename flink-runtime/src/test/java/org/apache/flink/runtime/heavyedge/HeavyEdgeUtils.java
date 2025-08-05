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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flink.runtime.heavyedge;

import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;

import java.util.Objects;

final class HeavyEdgeUtils {

    /** Counts pipelined edges whose producer and consumer land on different TMs. */
    static long countHeavyEdges(ArchivedExecutionGraph graph) {
        return graph.getAllExecutionVertices().stream()
                .flatMap(v -> v.getProducedPartitions().values().stream())
                .filter(p -> p.getPartitionType().isPipelined())
                .filter(p -> {
                    ArchivedExecutionVertex prod = p.getProducer();
                    ArchivedExecutionVertex cons =
                            p.getConsumers().iterator().next().getCurrentExecutionAttempt().getVertex();
                    if (prod.getCurrentAssignedResourceLocation() == null ||
                        cons.getCurrentAssignedResourceLocation() == null) {
                        return true;    // unassigned yet → counts as heavy
                    }
                    return !Objects.equals(
                            prod.getCurrentAssignedResourceLocation().getResourceID(),
                            cons.getCurrentAssignedResourceLocation().getResourceID());
                })
                .count();
    }

    private HeavyEdgeUtils() {}
}
