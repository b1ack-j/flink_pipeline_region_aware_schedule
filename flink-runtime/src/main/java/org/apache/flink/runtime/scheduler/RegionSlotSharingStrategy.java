package org.apache.flink.runtime.scheduler;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingPipelinedRegion;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Slot sharing strategy that groups vertices by their pipeline region.
 * Each region will share slots so that all contained vertices prefer to run
 * on the same TaskManager.
 */
@Experimental
class RegionSlotSharingStrategy extends AbstractSlotSharingStrategy {

    RegionSlotSharingStrategy(
            SchedulingTopology topology,
            Set<SlotSharingGroup> logicalSlotSharingGroups,
            Set<CoLocationGroup> coLocationGroups) {
        super(topology, logicalSlotSharingGroups, coLocationGroups);
    }

    static class Factory implements SlotSharingStrategy.Factory {
        @Override
        public SlotSharingStrategy create(
                SchedulingTopology topology,
                Set<SlotSharingGroup> logicalSlotSharingGroups,
                Set<CoLocationGroup> coLocationGroups) {
            return new RegionSlotSharingStrategy(topology, logicalSlotSharingGroups, coLocationGroups);
        }
    }

    @Override
    protected Map<ExecutionVertexID, ExecutionSlotSharingGroup> computeExecutionSlotSharingGroups(
            SchedulingTopology schedulingTopology) {
        Map<ExecutionVertexID, ExecutionSlotSharingGroup> result = new HashMap<>();
        for (SchedulingPipelinedRegion region : schedulingTopology.getAllPipelinedRegions()) {
            Map<Integer, ExecutionSlotSharingGroup> groupsByIndex = new HashMap<>();
            for (SchedulingExecutionVertex vertex : region.getVertices()) {
                int subtask = vertex.getId().getSubtaskIndex();
                ExecutionSlotSharingGroup group =
                        groupsByIndex.computeIfAbsent(
                                subtask,
                                i -> new ExecutionSlotSharingGroup(new SlotSharingGroup()));
                group.addVertex(vertex.getId());
                result.put(vertex.getId(), group);
            }
        }
        return result;
    }
}
