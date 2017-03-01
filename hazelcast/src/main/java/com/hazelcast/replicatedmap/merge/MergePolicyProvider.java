/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.merge;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.nio.ClassLoaderUtil.newInstance;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * A provider for {@link ReplicatedMapMergePolicy} instances.
 */
public final class MergePolicyProvider {

    private final ConcurrentMap<String, ReplicatedMapMergePolicy> mergePolicyMap;

    private final NodeEngine nodeEngine;

    private final ConstructorFunction<String, ReplicatedMapMergePolicy> policyConstructorFunction
            = new ConstructorFunction<String, ReplicatedMapMergePolicy>() {
        @Override
        public ReplicatedMapMergePolicy createNew(String className) {
            try {
                return newInstance(nodeEngine.getConfigClassLoader(), className);
            } catch (Exception e) {
                nodeEngine.getLogger(getClass()).severe(e);
                throw ExceptionUtil.rethrow(e);
            }
        }
    };

    public MergePolicyProvider(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        mergePolicyMap = new ConcurrentHashMap<String, ReplicatedMapMergePolicy>();
        addOutOfBoxPolicies();
    }

    private void addOutOfBoxPolicies() {
        mergePolicyMap.put(PutIfAbsentMapMergePolicy.class.getName(), PutIfAbsentMapMergePolicy.INSTANCE);
        mergePolicyMap.put(HigherHitsMapMergePolicy.class.getName(), HigherHitsMapMergePolicy.INSTANCE);
        mergePolicyMap.put(PassThroughMergePolicy.class.getName(), PassThroughMergePolicy.INSTANCE);
        mergePolicyMap.put(LatestUpdateMapMergePolicy.class.getName(), LatestUpdateMapMergePolicy.INSTANCE);
    }

    public ReplicatedMapMergePolicy getMergePolicy(String className) {
        checkNotNull(className, "Class name is mandatory!");
        return ConcurrencyUtil.getOrPutIfAbsent(mergePolicyMap, className, policyConstructorFunction);
    }
}
