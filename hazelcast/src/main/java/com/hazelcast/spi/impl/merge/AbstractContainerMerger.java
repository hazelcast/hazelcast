/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.impl.merge;

import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.merge.MergingValue;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicyProvider;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

/**
 * Merges data structures which have been collected via an {@link AbstractContainerCollector}.
 *
 * @param <C> container of the data structure
 * @param <V> the type of the merged value
 * @param <T> the type of the merging value, e.g. {@code MergingValue} or {@code MergingEntry & MergingHits}
 */
public abstract class AbstractContainerMerger<C, V, T extends MergingValue<V>> implements Runnable {

    private static final long TIMEOUT_FACTOR = 500;
    private static final long MINIMAL_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(5);

    protected final AbstractContainerCollector<C> collector;

    private final Semaphore semaphore = new Semaphore(0);
    private final BiConsumer<Object, Throwable> mergeCallback;

    private final ILogger logger;
    private final OperationService operationService;
    private final SplitBrainMergePolicyProvider splitBrainMergePolicyProvider;

    private int operationCount;

    protected AbstractContainerMerger(AbstractContainerCollector<C> collector, NodeEngine nodeEngine) {
        this.collector = collector;
        this.logger = nodeEngine.getLogger(AbstractContainerMerger.class);
        this.mergeCallback = (response, t) -> {
            if (t == null) {
                semaphore.release(1);
            } else {
                logger.warning("Error while running " + getLabel() + " merge operation: " + t.getMessage());
                semaphore.release(1);
            }
        };
        this.operationService = nodeEngine.getOperationService();
        this.splitBrainMergePolicyProvider = nodeEngine.getSplitBrainMergePolicyProvider();
    }

    @Override
    public final void run() {
        int valueCount = collector.getMergingValueCount();
        if (valueCount == 0) {
            return;
        }

        runInternal();

        assert operationCount > 0 : "No merge operations have been invoked in AbstractContainerMerger";

        try {
            long timeoutMillis = Math.max(valueCount * TIMEOUT_FACTOR, MINIMAL_TIMEOUT_MILLIS);
            if (!semaphore.tryAcquire(operationCount, timeoutMillis, TimeUnit.MILLISECONDS)) {
                logger.warning("Split-brain healing for " + getLabel() + " didn't finish within the timeout...");
            }
        } catch (InterruptedException e) {
            logger.finest("Interrupted while waiting for split-brain healing of " + getLabel() + "...");
            Thread.currentThread().interrupt();
        } finally {
            collector.destroy();
        }
    }

    /**
     * Returns a label of the service for customized error messages.
     */
    protected abstract String getLabel();

    /**
     * Executes the service specific merging logic.
     */
    protected abstract void runInternal();

    /**
     * Returns the {@link SplitBrainMergePolicy} instance of a given {@link MergePolicyConfig}.
     *
     * @param mergePolicyConfig the {@link MergePolicyConfig} to retrieve the merge policy from
     * @return the {@link SplitBrainMergePolicy} instance
     */
    protected <R> SplitBrainMergePolicy<V, T, R> getMergePolicy(MergePolicyConfig mergePolicyConfig) {
        String mergePolicyName = mergePolicyConfig.getPolicy();
        return splitBrainMergePolicyProvider.getMergePolicy(mergePolicyName);
    }

    /**
     * Invokes the given merge operation.
     *
     * @param serviceName the service name
     * @param operation   the merge operation
     * @param partitionId the partition ID of the operation
     */
    protected void invoke(String serviceName, Operation operation, int partitionId) {
        try {
            operationCount++;
            operationService
                    .invokeOnPartition(serviceName, operation, partitionId)
                    .whenCompleteAsync(mergeCallback);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }
}
