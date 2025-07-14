package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.slotpool.FreeSlotTracker;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Slot selection strategy that packs slots onto as few TaskManagers as possible.
 */
class RegionPackingSlotSelectionStrategy extends LocationPreferenceSlotSelectionStrategy {

    @Nonnull
    @Override
    protected Optional<SlotInfoAndLocality> selectWithoutLocationPreference(
            @Nonnull FreeSlotTracker freeSlotTracker, @Nonnull ResourceProfile resourceProfile) {
        Map<ResourceID, Integer> freePerTm = new HashMap<>();
        for (PhysicalSlot slot : freeSlotTracker.getFreeSlotsInformation()) {
            ResourceID tm = slot.getTaskManagerLocation().getResourceID();
            if (slot.getResourceProfile().isMatching(resourceProfile)) {
                freePerTm.merge(tm, 1, Integer::sum);
            }
        }
        SlotInfo bestCandidate = null;
        int bestCount = -1;
        for (AllocationID id : freeSlotTracker.getAvailableSlots()) {
            SlotInfo info = freeSlotTracker.getSlotInfo(id);
            if (!info.getResourceProfile().isMatching(resourceProfile)) {
                continue;
            }
            ResourceID tm = info.getTaskManagerLocation().getResourceID();
            int count = freePerTm.getOrDefault(tm, 0);
            if (count > bestCount) {
                bestCount = count;
                bestCandidate = info;
            }
        }
        return bestCandidate == null
                ? Optional.empty()
                : Optional.of(SlotInfoAndLocality.of(bestCandidate, Locality.UNCONSTRAINED));
    }

    @Override
    protected double calculateCandidateScore(
            int localWeigh, int hostLocalWeigh, java.util.function.Supplier<Double> taskExecutorUtilizationSupplier) {
        return -taskExecutorUtilizationSupplier.get();
    }
}
