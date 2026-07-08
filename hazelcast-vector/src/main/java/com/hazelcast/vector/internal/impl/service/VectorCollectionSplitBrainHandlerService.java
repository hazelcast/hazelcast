/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.internal.impl.service;

import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.merge.AbstractMergeRunnable;
import com.hazelcast.spi.impl.merge.AbstractSplitBrainHandlerService;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.VectorCollectionMergeTypes;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.internal.impl.VectorCollectionService;
import com.hazelcast.vector.internal.impl.ops.MergeOperationFactory;
import com.hazelcast.vector.internal.impl.storage.VectorCollectionStorage;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;

public class VectorCollectionSplitBrainHandlerService extends AbstractSplitBrainHandlerService<VectorCollectionStorage> {

    private final NodeEngine nodeEngine;

    public VectorCollectionSplitBrainHandlerService(NodeEngine nodeEngine) {
        super(nodeEngine);
        this.nodeEngine = nodeEngine;
    }

    @Override
    protected Runnable newMergeRunnable(Collection<VectorCollectionStorage> mergingStores) {
        return new VectorCollectionMergeRunnable(mergingStores);
    }

    @Override
    protected Iterator<VectorCollectionStorage> storeIterator(int partitionId) {
        return getService().storageIterator(partitionId);
    }

    @Override
    protected void destroyStore(VectorCollectionStorage vectorCollectionStorage) {
        // TODO: properly destroy VectorCollectionStorage (important for HD)
    }

    @Override
    protected boolean hasEntries(VectorCollectionStorage vectorCollectionStorage) {
        return !vectorCollectionStorage.isEmpty();
    }

    @Override
    protected boolean hasMergeablePolicy(VectorCollectionStorage vectorCollectionStorage) {
        String namespace = vectorCollectionStorage.getConfig().getUserCodeNamespace();
        String policy = vectorCollectionStorage.getConfig().getMergePolicyConfig().getPolicy();
        Object mergePolicy = nodeEngine.getSplitBrainMergePolicyProvider().getMergePolicy(policy, namespace);
        return !(mergePolicy instanceof DiscardMergePolicy);
    }

    VectorCollectionServiceImpl getService() {
        return nodeEngine.getService(VectorCollectionService.SERVICE_NAME);
    }

    class VectorCollectionMergeRunnable extends AbstractMergeRunnable<Object, VectorDocument<?>,
            VectorCollectionStorage, VectorCollectionMergeTypes<Object, VectorDocument<?>>> {

        VectorCollectionMergeRunnable(Collection<VectorCollectionStorage> mergingStores) {
            super(VectorCollectionService.SERVICE_NAME, mergingStores, VectorCollectionSplitBrainHandlerService.this, nodeEngine);
        }

        @Override
        protected void mergeStore(VectorCollectionStorage recordStore,
                                  BiConsumer<Integer, VectorCollectionMergeTypes<Object, VectorDocument<?>>> consumer) {
            recordStore.consumeAll(consumer);
        }

        @Override
        protected int getBatchSize(String dataStructureName) {
            return getService().getExistingVectorCollectionConfig(dataStructureName)
                    .getMergePolicyConfig().getBatchSize();
        }

        @Override
        protected SplitBrainMergePolicy<VectorDocument<?>,
                VectorCollectionMergeTypes<Object, VectorDocument<?>>, Object> getMergePolicy(String dataStructureName) {
            VectorCollectionConfig config = getService().getExistingVectorCollectionConfig(dataStructureName);
            var mergePolicyConfig = config.getMergePolicyConfig();
            return mergePolicyProvider.getMergePolicy(mergePolicyConfig.getPolicy(), config.getUserCodeNamespace());
        }

        @Override
        protected String getDataStructureName(VectorCollectionStorage vectorCollectionStorage) {
            return vectorCollectionStorage.getName();
        }

        @Override
        protected int getPartitionId(VectorCollectionStorage vectorCollectionStorage) {
            return vectorCollectionStorage.getPartitionId();
        }

        @Override
        protected OperationFactory createMergeOperationFactory(String dataStructureName,
                               SplitBrainMergePolicy<VectorDocument<?>, VectorCollectionMergeTypes<Object, VectorDocument<?>>,
                               Object> mergePolicy, int[] partitions,
                               List<VectorCollectionMergeTypes<Object, VectorDocument<?>>>[] entries) {
            return new MergeOperationFactory(dataStructureName, partitions, entries, mergePolicy);
        }
    }
}
