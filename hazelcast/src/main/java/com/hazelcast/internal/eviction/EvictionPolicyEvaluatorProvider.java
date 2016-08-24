/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.eviction;

import com.hazelcast.internal.eviction.impl.comparator.LFUEvictionPolicyComparator;
import com.hazelcast.internal.eviction.impl.comparator.LRUEvictionPolicyComparator;
import com.hazelcast.internal.eviction.impl.evaluator.DefaultEvictionPolicyEvaluator;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.StringUtil;

/**
 * Provider to get any kind ({@link EvictionPolicyType}) of {@link EvictionPolicyEvaluator}.
 */
public final class EvictionPolicyEvaluatorProvider {

    private EvictionPolicyEvaluatorProvider() {
    }

    private static EvictionPolicyComparator createEvictionPolicyComparator(EvictionPolicyType evictionPolicyType) {
        switch (evictionPolicyType) {
            case LRU:
                return new LRUEvictionPolicyComparator();
            case LFU:
                return new LFUEvictionPolicyComparator();
            default:
                throw new IllegalArgumentException("Unsupported eviction policy type: " + evictionPolicyType);
        }
    }

    /**
     * Gets the {@link EvictionPolicyEvaluator} implementation specified with <code>evictionPolicy</code>.
     *
     * @param evictionConfig {@link EvictionConfiguration} for requested {@link EvictionPolicyEvaluator} implementation
     * @param classLoader    the {@link java.lang.ClassLoader} to be used
     *                       while creating custom {@link EvictionPolicyComparator} if it is specified in the config
     * @return the requested {@link EvictionPolicyEvaluator} implementation
     */
    public static EvictionPolicyEvaluator getEvictionPolicyEvaluator(EvictionConfiguration evictionConfig,
                                                                     ClassLoader classLoader) {
        if (evictionConfig == null) {
            return null;
        }

        EvictionPolicyComparator evictionPolicyComparator = null;

        String evictionPolicyComparatorClassName = evictionConfig.getComparatorClassName();
        if (!StringUtil.isNullOrEmpty(evictionPolicyComparatorClassName)) {
            try {
                evictionPolicyComparator = ClassLoaderUtil.newInstance(classLoader, evictionPolicyComparatorClassName);
            } catch (Exception e) {
                ExceptionUtil.rethrow(e);
            }
        } else {
            EvictionPolicyComparator comparator = evictionConfig.getComparator();
            if (comparator != null) {
                evictionPolicyComparator = comparator;
            } else {
                EvictionPolicyType evictionPolicyType = evictionConfig.getEvictionPolicyType();
                if (evictionPolicyType == null) {
                    return null;
                }
                evictionPolicyComparator = createEvictionPolicyComparator(evictionPolicyType);
            }
        }

        return new DefaultEvictionPolicyEvaluator(evictionPolicyComparator);
    }
}
