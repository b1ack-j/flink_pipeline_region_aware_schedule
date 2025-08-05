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

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.TestInvokable;

final class HeavyEdgeTestJobGenerator {

    static JobGraph hashShuffleJob(int upstreamPar, int downstreamPar) {
        JobVertex src = new JobVertex("source");
        src.setInvokableClass(TestInvokable.class);
        src.setParallelism(upstreamPar);

        JobVertex map = new JobVertex("map");
        map.setInvokableClass(TestInvokable.class);
        map.setParallelism(upstreamPar);

        JobVertex agg = new JobVertex("windowAgg");
        agg.setInvokableClass(TestInvokable.class);
        agg.setParallelism(downstreamPar);

        JobVertex sink = new JobVertex("sink");
        sink.setInvokableClass(TestInvokable.class);
        sink.setParallelism(downstreamPar);

        // chain: src -> map (forward)
        JobGraphTestUtils.connect(
                src, map, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
        // map -> agg (HASH / ALL_TO_ALL)
        JobGraphTestUtils.connect(
                map, agg, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        // agg -> sink (forward)
        JobGraphTestUtils.connect(
                agg, sink, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        return JobGraphTestUtils.streamingJobGraph(src, map, agg, sink);
    }

    private HeavyEdgeTestJobGenerator() {}
}