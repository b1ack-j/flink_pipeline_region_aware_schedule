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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.scheduler.DefaultSchedulerBuilder;
import org.apache.flink.runtime.scheduler.strategy.PipelinedRegionSchedulingStrategy;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Quick functional benchmark of the **stock** Flink scheduler.
 *
 * Run with:  mvn -pl flink-runtime test -Dtest=DefaultSchedulerBenchTest
 */
class DefaultSchedulerBenchTest {

    /** Builds a small but non‑trivial streaming job:
     *  Source(300) -> Map(300) -> HASH -> WindowAgg(300) -> Sink(300)
     */
    private static JobGraph buildHashShuffleJob() {
        return HeavyEdgeTestJobGenerator.hashShuffleJob(300, 300);   // util shown below
    }

    @Test
    void measureDefaultScheduler() throws Exception {
        JobGraph jobGraph = buildHashShuffleJob();

        ExecutorService ioPool = Executors.newFixedThreadPool(2);
        var main =
                ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                        new DirectScheduledExecutorService());

        DefaultScheduler scheduler =
                new DefaultSchedulerBuilder(jobGraph, main, ioPool, ioPool, ioPool)
                        .setSchedulingStrategyFactory(new PipelinedRegionSchedulingStrategy.Factory())
                        .build();

        /* ---------- measure latency ---------- */
        long t0 = System.nanoTime();
        main.execute(scheduler::startScheduling);

        // Busy‑wait until the job has at least left the CREATED state
        while (scheduler.requestJobStatus() == JobStatus.CREATED) {
            Thread.yield();
        }
        long latencyMs = (System.nanoTime() - t0) / 1_000_000;

        /* ---------- count heavy edges -------- */
        ArchivedExecutionGraph g = scheduler.requestJob().getArchivedExecutionGraph();
        long heavyEdges = HeavyEdgeUtils.countHeavyEdges(g);     // util shown below

        System.out.printf(
                "Default scheduler: latency=%d ms, heavyEdges=%d%n", latencyMs, heavyEdges);

        // optional assertion – adapt numbers once you know the baseline
        assertThat(heavyEdges).isPositive();

        scheduler.closeAsync().join();
        ioPool.shutdownNow();
    }
}
