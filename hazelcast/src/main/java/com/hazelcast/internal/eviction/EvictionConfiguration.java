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

/**
 * Interface for configuration information about eviction.
 */
public interface EvictionConfiguration {

    /**
     * Gets the type of eviction strategy.
     *
     * @return the type of eviction strategy
     */
    EvictionStrategyType getEvictionStrategyType();

    /**
     * Gets the eviction policy.
     *
     * @return the eviction policy
     */
    EvictionPolicy getEvictionPolicy();

    /**
     * Gets the type of eviction policy.
     *
     * @return the type of eviction policy
     * @deprecated since 3.9, use {@link #getEvictionPolicy()} instead
     */
    @Deprecated
    EvictionPolicyType getEvictionPolicyType();

    /**
     * Gets the class name of the configured {@link EvictionPolicyComparator} implementation.
     *
     * @return class name of the configured {@link EvictionPolicyComparator} implementation
     */
    String getComparatorClassName();

    /**
     * Gets instance of the configured {@link EvictionPolicyComparator} implementation.
     *
     * @return instance of the configured {@link EvictionPolicyComparator} implementation.
     */
    EvictionPolicyComparator getComparator();
}
