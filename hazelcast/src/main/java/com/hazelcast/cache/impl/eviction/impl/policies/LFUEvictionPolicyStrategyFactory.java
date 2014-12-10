/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.eviction.impl.policies;

import com.hazelcast.cache.impl.eviction.EvictionPolicyStrategy;
import com.hazelcast.cache.impl.eviction.policies.LFUSingleEvictionPolicyStrategy;

import javax.cache.configuration.Factory;

public class LFUEvictionPolicyStrategyFactory
        implements Factory<EvictionPolicyStrategy> {

    private static final EvictionPolicyStrategy EVICTION_POLICY_STRATEGY = new LFUSingleEvictionPolicyStrategy();

    @Override
    public EvictionPolicyStrategy create() {
        return EVICTION_POLICY_STRATEGY;
    }
}
