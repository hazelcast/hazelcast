/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.merge;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicyProvider;
import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.nio.ClassLoaderUtil.newInstance;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

/**
 * A provider for {@link com.hazelcast.map.merge.MapMergePolicy} instances.
 */
public final class MergePolicyProvider {

    private final ConcurrentMap<String, MapMergePolicy> mergePolicyMap = new ConcurrentHashMap<String, MapMergePolicy>();

    private final ConstructorFunction<String, MapMergePolicy> policyConstructorFunction
            = new ConstructorFunction<String, MapMergePolicy>() {
        @Override
        public MapMergePolicy createNew(String className) {
            try {
                return newInstance(nodeEngine.getConfigClassLoader(), className);
            } catch (Exception e) {
                throw new InvalidConfigurationException("Invalid MapMergePolicy: " + className, e);
            }
        }
    };

    private final NodeEngine nodeEngine;
    private final SplitBrainMergePolicyProvider policyProvider;

    public MergePolicyProvider(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.policyProvider = nodeEngine.getSplitBrainMergePolicyProvider();
        addOutOfBoxPolicies();
    }

    private void addOutOfBoxPolicies() {
        mergePolicyMap.put(PutIfAbsentMapMergePolicy.class.getName(), new PutIfAbsentMapMergePolicy());
        mergePolicyMap.put(HigherHitsMapMergePolicy.class.getName(), new HigherHitsMapMergePolicy());
        mergePolicyMap.put(PassThroughMergePolicy.class.getName(), new PassThroughMergePolicy());
        mergePolicyMap.put(LatestUpdateMapMergePolicy.class.getName(), new LatestUpdateMapMergePolicy());
    }

    /**
     * Returns an instance of a merge policy by its classname.
     * <p>
     * First tries to resolve the classname as {@link SplitBrainMergePolicy},
     * then as {@link com.hazelcast.map.merge.MapMergePolicy}.
     * <p>
     * If no merge policy matches an exception is thrown.
     *
     * @param className the classname of the given merge policy
     * @return an instance of the merge policy class
     */
    public Object getMergePolicy(String className) {
        if (className == null) {
            throw new InvalidConfigurationException("Class name is mandatory!");
        }
        try {
            return policyProvider.getMergePolicy(className);
        } catch (InvalidConfigurationException e) {
            return getOrPutIfAbsent(mergePolicyMap, className, policyConstructorFunction);
        }
    }
}
