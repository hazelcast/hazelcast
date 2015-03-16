package com.hazelcast.spi.impl.operationexecutor.progressive;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.executor.HazelcastManagedThread;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public final class PartitionIdScanThread extends HazelcastManagedThread {
    private static final int SCAN_DELAY = 1000;

    private final Set<Integer> set = new HashSet<Integer>();
    private final NodeEngine nodeEngine;
    private final AtomicReference<int[]> localPartitionIds;
    private volatile boolean shutdown;

    public PartitionIdScanThread(HazelcastThreadGroup threadGroup, NodeEngine nodeEngine,
                                 AtomicReference<int[]> localPartitionIds) {
        super(threadGroup.getInternalThreadGroup(), "partitionIdScanner");
        this.nodeEngine = nodeEngine;
        this.localPartitionIds = localPartitionIds;
    }

    public void shutdown() {
        shutdown = true;
        interrupt();
    }

    @Override
    public void run() {
        while (!shutdown) {
            set.clear();

            // quite inefficient implementation.
            for (InternalPartition partition : nodeEngine.getPartitionService().getPartitions()) {
                if (partition.isLocal()) {
                    set.add(partition.getPartitionId());
                }
            }

            int[] array = new int[set.size()];
            Iterator<Integer> it = set.iterator();
            for (int k = 0; k < array.length; k++) {
                array[k] = it.next();
            }

            localPartitionIds.set(array);

            try {
                Thread.sleep(SCAN_DELAY);
            } catch (InterruptedException ignore) {
            }
        }
    }
}
