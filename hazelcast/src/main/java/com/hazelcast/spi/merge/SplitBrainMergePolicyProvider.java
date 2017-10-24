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
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.nio.ClassLoaderUtil.newInstance;

/**
 * A provider for {@link SplitBrainMergePolicy} instances.
 * <p>
 * Registers out-of-the-box merge policies by their fully qualified and simple class name.
 */
public final class SplitBrainMergePolicyProvider {

    private static final Map<String, SplitBrainMergePolicy> OUT_OF_THE_BOX_MERGE_POLICIES;

    static {
        OUT_OF_THE_BOX_MERGE_POLICIES = new HashMap<String, SplitBrainMergePolicy>();
        OUT_OF_THE_BOX_MERGE_POLICIES.put(DiscardMergePolicy.class.getName(), new DiscardMergePolicy());
        OUT_OF_THE_BOX_MERGE_POLICIES.put(DiscardMergePolicy.class.getSimpleName(), new DiscardMergePolicy());
        OUT_OF_THE_BOX_MERGE_POLICIES.put(HigherHitsMergePolicy.class.getName(), new HigherHitsMergePolicy());
        OUT_OF_THE_BOX_MERGE_POLICIES.put(HigherHitsMergePolicy.class.getSimpleName(), new HigherHitsMergePolicy());
        OUT_OF_THE_BOX_MERGE_POLICIES.put(LatestAccessMergePolicy.class.getName(), new LatestAccessMergePolicy());
        OUT_OF_THE_BOX_MERGE_POLICIES.put(LatestAccessMergePolicy.class.getSimpleName(), new LatestAccessMergePolicy());
        OUT_OF_THE_BOX_MERGE_POLICIES.put(LatestUpdateMergePolicy.class.getName(), new LatestUpdateMergePolicy());
        OUT_OF_THE_BOX_MERGE_POLICIES.put(LatestUpdateMergePolicy.class.getSimpleName(), new LatestUpdateMergePolicy());
        OUT_OF_THE_BOX_MERGE_POLICIES.put(PassThroughMergePolicy.class.getName(), new PassThroughMergePolicy());
        OUT_OF_THE_BOX_MERGE_POLICIES.put(PassThroughMergePolicy.class.getSimpleName(), new PassThroughMergePolicy());
        OUT_OF_THE_BOX_MERGE_POLICIES.put(PutIfAbsentMergePolicy.class.getName(), new PutIfAbsentMergePolicy());
        OUT_OF_THE_BOX_MERGE_POLICIES.put(PutIfAbsentMergePolicy.class.getSimpleName(), new PutIfAbsentMergePolicy());
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
        return ConcurrencyUtil.getOrPutIfAbsent(mergePolicyMap, className, policyConstructorFunction);
    }
}
