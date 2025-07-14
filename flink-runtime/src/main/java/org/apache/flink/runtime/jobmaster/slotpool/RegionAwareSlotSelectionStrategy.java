package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Slot selection strategy that tries to place subsequent requests on the fewest
 * possible TaskManagers by preferring the ones with the most free slots.
 */
class RegionAwareSlotSelectionStrategy extends LocationPreferenceSlotSelectionStrategy {

    @Nonnull
    @Override
    protected Optional<SlotInfoAndLocality> selectWithoutLocationPreference(
            @Nonnull FreeSlotTracker freeSlotTracker, @Nonnull ResourceProfile resourceProfile) {
        Map<ResourceID, List<SlotInfo>> slotsByTm =
                freeSlotTracker.getAvailableSlots().stream()
                        .map(freeSlotTracker::getSlotInfo)
                        .filter(slotInfo -> slotInfo.getResourceProfile().isMatching(resourceProfile))
                        .collect(Collectors.groupingBy(slot -> slot.getTaskManagerLocation().getResourceID()));

        return slotsByTm.entrySet().stream()
                .map(entry -> Tuple2.of(entry.getKey(), entry.getValue()))
                .max(Comparator.<Tuple2<ResourceID, List<SlotInfo>>>comparingInt(t -> t.f1.size())
                        .thenComparingDouble(t -> freeSlotTracker.getTaskExecutorUtilization(t.f1.get(0)))
                        .reversed())
                .map(best -> SlotInfoAndLocality.of(best.f1.get(0), Locality.UNCONSTRAINED));
    }

    @Override
    protected double calculateCandidateScore(
            int localWeigh, int hostLocalWeigh, Supplier<Double> taskExecutorUtilizationSupplier) {
        return localWeigh * 20 + hostLocalWeigh * 2 - taskExecutorUtilizationSupplier.get();
    }
}
