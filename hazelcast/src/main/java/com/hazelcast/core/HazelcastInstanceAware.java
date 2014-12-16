/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

/**
 * Used to get HazelcastInstance reference when submitting a Runnable/Callable using Hazelcast ExecutorService.
 * Before executing the Runnable/Callable Hazelcast will invoke
 * {@link HazelcastInstanceAware#setHazelcastInstance(HazelcastInstance)}  method with the reference to HazelcastInstance
 * that is executing. This way the implementer will have a chance to get the reference to HazelcastInstance.
 */
public interface HazelcastInstanceAware {

    /**
     * Gets the HazelcastInstance reference when submitting a Runnable/Callable using Hazelcast ExecutorService.
     *
     * @param hazelcastInstance the HazelcastInstance reference
     */
    void setHazelcastInstance(HazelcastInstance hazelcastInstance);
}
