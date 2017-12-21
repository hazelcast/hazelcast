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

package com.hazelcast.spi.merge;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.util.ConstructorFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.nio.ClassLoaderUtil.newInstance;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

/**
 * A provider for {@link SplitBrainMergePolicy} instances.
 * <p>
 * Registers out-of-the-box merge policies by their fully qualified and simple class name.
 */
public final class SplitBrainMergePolicyProvider {

    private static final Map<String, SplitBrainMergePolicy> OUT_OF_THE_BOX_MERGE_POLICIES;

    static {
        OUT_OF_THE_BOX_MERGE_POLICIES = new HashMap<String, SplitBrainMergePolicy>();
        addPolicy(DiscardMergePolicy.class, new DiscardMergePolicy());
        addPolicy(HigherHitsMergePolicy.class, new HigherHitsMergePolicy());
        addPolicy(LatestAccessMergePolicy.class, new LatestAccessMergePolicy());
        addPolicy(LatestUpdateMergePolicy.class, new LatestUpdateMergePolicy());
        addPolicy(PassThroughMergePolicy.class, new PassThroughMergePolicy());
        addPolicy(PutIfAbsentMergePolicy.class, new PutIfAbsentMergePolicy());
    }

    private final NodeEngine nodeEngine;

    private final ConcurrentMap<String, SplitBrainMergePolicy> mergePolicyMap
            = new ConcurrentHashMap<String, SplitBrainMergePolicy>();

    private final ConstructorFunction<String, SplitBrainMergePolicy> policyConstructorFunction
            = new ConstructorFunction<String, SplitBrainMergePolicy>() {
        @Override
        public SplitBrainMergePolicy createNew(String className) {
            try {
                return newInstance(nodeEngine.getConfigClassLoader(), className);
            } catch (Exception e) {
                nodeEngine.getLogger(getClass()).severe(e);
                throw new InvalidConfigurationException("Invalid SplitBrainMergePolicy: " + className, e);
            }
        }
    };

    public SplitBrainMergePolicyProvider(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.mergePolicyMap.putAll(OUT_OF_THE_BOX_MERGE_POLICIES);
    }

    public SplitBrainMergePolicy getMergePolicy(String className) {
        if (className == null) {
            throw new InvalidConfigurationException("Class name is mandatory!");
        }

        return getOrPutIfAbsent(mergePolicyMap, className, policyConstructorFunction);
    }

    private static <T extends SplitBrainMergePolicy> void addPolicy(Class<T> clazz, T policy) {
        OUT_OF_THE_BOX_MERGE_POLICIES.put(clazz.getName(), policy);
        OUT_OF_THE_BOX_MERGE_POLICIES.put(clazz.getSimpleName(), policy);
    }
}
