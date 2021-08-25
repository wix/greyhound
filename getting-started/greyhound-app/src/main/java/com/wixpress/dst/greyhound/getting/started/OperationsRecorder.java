package com.wixpress.dst.greyhound.getting.started;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class OperationsRecorder {
    public OperationsRecorder() {
    }

    private AtomicReference<HashMap<Integer, ArrayList<Integer>>> operationsPerPartition;

    public void recordOperation(int partition, int sizeOfBatch) {
        operationsPerPartition.updateAndGet(acc -> {
            try {
                ArrayList<Integer> sizes = acc.getOrDefault(partition, new ArrayList<>());
                sizes.add(sizeOfBatch);
                acc.put(partition, sizes);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return acc;
        });
    }

    public Set<Map.Entry<Integer, ArrayList<Integer>>> operationsPerPartitionEntries() {
        return operationsPerPartition.get().entrySet();
    }

    public int totalEntries() {
        return operationsPerPartitionEntries().stream()
                .map(e -> e.getValue().stream().reduce(0, Integer::sum))
                .reduce(0, Integer::sum);
    }

    public int totalOperations() {
        return operationsPerPartitionEntries().stream()
                .map(e -> e.getValue().size())
                .reduce(0, Integer::sum);
    }

    public void reset() {
        operationsPerPartition = new AtomicReference<>(new HashMap<>());
    }
}
