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

package com.hazelcast.internal.eviction;

import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.internal.eviction.impl.comparator.LFUEvictionPolicyComparator;
import com.hazelcast.internal.eviction.impl.comparator.LRUEvictionPolicyComparator;
import com.hazelcast.internal.eviction.impl.comparator.RandomEvictionPolicyComparator;
import com.hazelcast.internal.eviction.impl.evaluator.EvictionPolicyEvaluator;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.spi.eviction.EvictionPolicyComparator;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;

/**
 * Provider to get any kind ({@link EvictionPolicy}) of {@link EvictionPolicyEvaluator}.
 */
public final class EvictionPolicyEvaluatorProvider {

    private EvictionPolicyEvaluatorProvider() {
    }

    /**
     * Gets the {@link EvictionPolicyEvaluator}
     * implementation specified with {@code evictionPolicy}.
     *
     * @param evictionConfig {@link EvictionConfiguration} for
     *                       requested {@link EvictionPolicyEvaluator} implementation
     * @param classLoader    the {@link java.lang.ClassLoader} to be
     *                       used while creating custom {@link EvictionPolicyComparator}
     *                       if it is specified in the config
     * @return the requested
     * {@link EvictionPolicyEvaluator} implementation
     */
    public static <A, E extends Evictable> EvictionPolicyEvaluator<A, E>
    getEvictionPolicyEvaluator(EvictionConfiguration evictionConfig, ClassLoader classLoader) {
        checkNotNull(evictionConfig);

        return new EvictionPolicyEvaluator<>(getEvictionPolicyComparator(evictionConfig, classLoader));
    }

    /**
     * @param evictionConfig {@link EvictionConfiguration} for
     *                       requested {@link EvictionPolicyEvaluator} implementation
     * @param classLoader    the {@link java.lang.ClassLoader} to be
     *                       used while creating custom {@link EvictionPolicyComparator}
     *                       if it is specified in the config
     * @return {@link
     * EvictionPolicyComparator} instance if it is defined, otherwise
     * returns null to indicate there is no comparator defined
     */
    public static EvictionPolicyComparator getEvictionPolicyComparator(EvictionConfiguration evictionConfig,
                                                                       ClassLoader classLoader) {
        // 1. First check comparator class name
        String evictionPolicyComparatorClassName = evictionConfig.getComparatorClassName();
        if (!isNullOrEmpty(evictionPolicyComparatorClassName)) {
            try {
                return ClassLoaderUtil.newInstance(classLoader, evictionPolicyComparatorClassName);
            } catch (Exception e) {
                throw rethrow(e);
            }
        }

        // 2. Then check comparator implementation
        EvictionPolicyComparator comparator = evictionConfig.getComparator();
        if (comparator != null) {
            return comparator;
        }

        // 3. As a last resort, try to pick an out-of-the-box comparator implementation
        return pickOutOfTheBoxComparator(evictionConfig.getEvictionPolicy());
    }

    private static EvictionPolicyComparator pickOutOfTheBoxComparator(EvictionPolicy evictionPolicy) {
        switch (evictionPolicy) {
            case LRU:
                return LRUEvictionPolicyComparator.INSTANCE;
            case LFU:
                return LFUEvictionPolicyComparator.INSTANCE;
            case RANDOM:
                return RandomEvictionPolicyComparator.INSTANCE;
            case NONE:
                return null;
            default:
                throw new IllegalArgumentException("Unsupported eviction policy: " + evictionPolicy);
        }
    }
}
