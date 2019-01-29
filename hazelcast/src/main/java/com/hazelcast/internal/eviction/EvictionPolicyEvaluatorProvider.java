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

package com.hazelcast.internal.eviction;

import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.internal.eviction.impl.comparator.LFUEvictionPolicyComparator;
import com.hazelcast.internal.eviction.impl.comparator.LRUEvictionPolicyComparator;
import com.hazelcast.internal.eviction.impl.comparator.RandomEvictionPolicyComparator;
import com.hazelcast.internal.eviction.impl.evaluator.EvictionPolicyEvaluator;
import com.hazelcast.nio.ClassLoaderUtil;

import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.StringUtil.isNullOrEmpty;

/**
 * Provider to get any kind ({@link EvictionPolicyType}) of {@link EvictionPolicyEvaluator}.
 */
public final class EvictionPolicyEvaluatorProvider {

    private EvictionPolicyEvaluatorProvider() {
    }

    private static EvictionPolicyComparator createEvictionPolicyComparator(EvictionPolicy evictionPolicy) {
        switch (evictionPolicy) {
            case LRU:
                return new LRUEvictionPolicyComparator();
            case LFU:
                return new LFUEvictionPolicyComparator();
            case RANDOM:
                return new RandomEvictionPolicyComparator();
            case NONE:
                return null;
            default:
                throw new IllegalArgumentException("Unsupported eviction policy: " + evictionPolicy);
        }
    }

    /**
     * Gets the {@link EvictionPolicyEvaluator} implementation specified with {@code evictionPolicy}.
     *
     * @param evictionConfig {@link EvictionConfiguration} for requested {@link EvictionPolicyEvaluator} implementation
     * @param classLoader    the {@link java.lang.ClassLoader} to be used
     *                       while creating custom {@link EvictionPolicyComparator} if it is specified in the config
     * @return the requested {@link EvictionPolicyEvaluator} implementation
     */
    public static <A, E extends Evictable> EvictionPolicyEvaluator<A, E> getEvictionPolicyEvaluator(
            EvictionConfiguration evictionConfig, ClassLoader classLoader) {
        checkNotNull(evictionConfig);

        EvictionPolicyComparator evictionPolicyComparator;

        String evictionPolicyComparatorClassName = evictionConfig.getComparatorClassName();
        if (!isNullOrEmpty(evictionPolicyComparatorClassName)) {
            try {
                evictionPolicyComparator = ClassLoaderUtil.newInstance(classLoader, evictionPolicyComparatorClassName);
            } catch (Exception e) {
                throw rethrow(e);
            }
        } else {
            EvictionPolicyComparator comparator = evictionConfig.getComparator();
            if (comparator != null) {
                evictionPolicyComparator = comparator;
            } else {
                evictionPolicyComparator = createEvictionPolicyComparator(evictionConfig.getEvictionPolicy());
            }
        }

        return new EvictionPolicyEvaluator<A, E>(evictionPolicyComparator);
    }
}
