package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.ResourceID;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

/**
 * RequestSlotMatchingStrategy that tries to allocate slots from task managers
 * with the most free slots first.
 */
public enum RegionAwareRequestSlotMatchingStrategy implements RequestSlotMatchingStrategy {
    INSTANCE;

    @Override
    public Collection<RequestSlotMatch> matchRequestsAndSlots(
            Collection<? extends PhysicalSlot> slots, Collection<PendingRequest> pendingRequests) {
        Map<ResourceID, Queue<PhysicalSlot>> byTm = new HashMap<>();
        for (PhysicalSlot slot : slots) {
            byTm.computeIfAbsent(slot.getTaskManagerLocation().getResourceID(), k -> new LinkedList<>())
                    .add(slot);
        }

        List<Map.Entry<ResourceID, Queue<PhysicalSlot>>> ordered =
                byTm.entrySet().stream()
                        .sorted((a, b) -> Integer.compare(b.getValue().size(), a.getValue().size()))
                        .collect(Collectors.toList());

        List<RequestSlotMatch> result = new ArrayList<>();
        Iterator<PendingRequest> requestIterator = new ArrayList<>(pendingRequests).iterator();
        for (Map.Entry<ResourceID, Queue<PhysicalSlot>> entry : ordered) {
            Queue<PhysicalSlot> tmSlots = entry.getValue();
            while (!tmSlots.isEmpty() && requestIterator.hasNext()) {
                PendingRequest request = requestIterator.next();
                PhysicalSlot slot = tmSlots.poll();
                if (slot.getResourceProfile().isMatching(request.getResourceProfile())) {
                    result.add(RequestSlotMatch.createFor(request, slot));
                    requestIterator.remove();
                }
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return RegionAwareRequestSlotMatchingStrategy.class.getSimpleName();
    }
}
