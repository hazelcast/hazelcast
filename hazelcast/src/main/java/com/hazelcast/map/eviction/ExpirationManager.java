package com.hazelcast.map.eviction;

import com.hazelcast.map.MapServiceContext;
import com.hazelcast.map.PartitionContainer;
import com.hazelcast.map.RecordStore;
import com.hazelcast.map.operation.ClearExpiredOperation;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.Clock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

// TODO refactor

/**
 * Manages expiration operations.
 *
 * @since 3.3
 */
public class ExpirationManager {

    private static final long INITIAL_DELAY = 5;

    private static final long PERIOD = 5;

    private static final TimeUnit UNIT = TimeUnit.SECONDS;

    private final NodeEngine nodeEngine;

    private final MapServiceContext mapServiceContext;

    public ExpirationManager(MapServiceContext mapServiceContext, NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.mapServiceContext = mapServiceContext;
    }

    public void start() {
        nodeEngine.getExecutionService()
                .scheduleAtFixedRate(new ClearExpiredRecordsTask(), INITIAL_DELAY, PERIOD, UNIT);
    }

    /**
     * Periodically clears expired entries.(ttl & idle)
     * This task provides per partition expiration operation logic. (not per map, not per record store).
     * Fires cleanup operations at most partition operation thread count or some factor of it in one round.
     */
    private class ClearExpiredRecordsTask implements Runnable {

        private static final int EXPIRATION_PERCENTAGE = 10;

        private static final long MIN_MILLIS_DIFF_BETWEEN_TWO_RUNS = 1000;

        private final Comparator<PartitionContainer> partitionContainerComparator = new Comparator<PartitionContainer>() {
            @Override
            public int compare(PartitionContainer o1, PartitionContainer o2) {
                final long s1 = o1.getLastCleanupTimeCopy();
                final long s2 = o2.getLastCleanupTimeCopy();
                return (s1 < s2) ? -1 : ((s1 == s2) ? 0 : 1);
            }
        };

        public void run() {
            final long now = Clock.currentTimeMillis();
            final NodeEngine nodeEngine = ExpirationManager.this.nodeEngine;
            final int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
            List<PartitionContainer> partitionContainers = Collections.emptyList();
            boolean createLazy = true;
            int currentlyRunningCleanupOperationsCount = 0;
            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                InternalPartition partition = nodeEngine.getPartitionService().getPartition(partitionId, false);
                if (partition.isOwnerOrBackup(nodeEngine.getThisAddress())) {
                    final PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
                    if (isContainerEmpty(partitionContainer)) {
                        continue;
                    }
                    if (hasRunningCleanup(partitionContainer)) {
                        currentlyRunningCleanupOperationsCount++;
                        continue;
                    }
                    if (currentlyRunningCleanupOperationsCount > getMaxCleanupOperationCountInOneRound()
                            || notInProcessableTimeWindow(partitionContainer, now)
                            || notAnyExpirableRecord(partitionContainer)) {
                        continue;
                    }

                    if (createLazy) {
                        partitionContainers = new ArrayList<PartitionContainer>();
                        createLazy = false;
                    }
                    partitionContainers.add(partitionContainer);
                }
            }
            if (partitionContainers.isEmpty()) {
                return;
            }

            sortPartitionContainers(partitionContainers);
            sendCleanupOperations(partitionContainers);
        }

        private void sortPartitionContainers(List<PartitionContainer> partitionContainers) {
            updateLastCleanupTimesBeforeSorting(partitionContainers);
            Collections.sort(partitionContainers, partitionContainerComparator);
        }


        private void sendCleanupOperations(List<PartitionContainer> partitionContainers) {
            final int maxCleanupOperationCountInOneRound = getMaxCleanupOperationCountInOneRound();
            final int start = 0;
            int end = maxCleanupOperationCountInOneRound;
            if (end > partitionContainers.size()) {
                end = partitionContainers.size();
            }
            final List<PartitionContainer> partitionIds = partitionContainers.subList(start, end);
            for (PartitionContainer container : partitionIds) {
                // mark partition container as has on going expiration operation.
                container.setHasRunningCleanup(true);
                OperationService operationService = ExpirationManager.this.nodeEngine.getOperationService();
                operationService.executeOperation(createExpirationOperation(EXPIRATION_PERCENTAGE,
                        container.getPartitionId()));
            }
        }

        private boolean expirable(RecordStore recordStore) {
            return recordStore.isExpirable();
        }

        private boolean hasRunningCleanup(PartitionContainer partitionContainer) {
            return partitionContainer.hasRunningCleanup();
        }

        private boolean notInProcessableTimeWindow(PartitionContainer partitionContainer, long now) {
            return now - partitionContainer.getLastCleanupTime() < MIN_MILLIS_DIFF_BETWEEN_TWO_RUNS;
        }

        private int getMaxCleanupOperationCountInOneRound() {
            final int times = 3;
            return times * ExpirationManager.this.nodeEngine.getOperationService().getPartitionOperationThreadCount();
        }

        private boolean isContainerEmpty(PartitionContainer container) {
            long size = 0L;
            final ConcurrentMap<String, RecordStore> maps = container.getMaps();
            for (RecordStore store : maps.values()) {
                size += store.size();
                if (size > 0L) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Here we check if that partition has any expirable record or not,
         * if no expirable record exists in that partition no need to fire an expiration operation.
         *
         * @param partitionContainer corresponding partition container.
         * @return <code>true</code> if no expirable record in that partition <code>false</code> otherwise.
         */
        private boolean notAnyExpirableRecord(PartitionContainer partitionContainer) {
            boolean notExist = true;
            final ConcurrentMap<String, RecordStore> maps = partitionContainer.getMaps();
            for (RecordStore store : maps.values()) {
                if (expirable(store)) {
                    notExist = false;
                    break;
                }
            }
            return notExist;
        }
    }

    private Operation createExpirationOperation(int expirationPercentage, int partitionId) {
        final ClearExpiredOperation clearExpiredOperation = new ClearExpiredOperation(expirationPercentage);
        clearExpiredOperation
                .setNodeEngine(nodeEngine)
                .setCallerUuid(nodeEngine.getLocalMember().getUuid())
                .setPartitionId(partitionId)
                .setValidateTarget(false)
                .setService(mapServiceContext.getService());
        return clearExpiredOperation;
    }


    /**
     * Sets last clean-up time before sorting.
     *
     * @param partitionContainers partition containers.
     */
    private void updateLastCleanupTimesBeforeSorting(List<PartitionContainer> partitionContainers) {
        for (PartitionContainer partitionContainer : partitionContainers) {
            partitionContainer.setLastCleanupTimeCopy(partitionContainer.getLastCleanupTime());
        }
    }


}
