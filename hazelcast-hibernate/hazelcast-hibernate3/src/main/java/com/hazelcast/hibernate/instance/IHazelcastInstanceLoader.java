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

package com.hazelcast.hibernate.instance;

import com.hazelcast.core.HazelcastInstance;
import org.hibernate.cache.CacheException;

import java.util.Properties;

/**
 * Factory interface to build Hazelcast instances and configure them depending
 * on configuration.
 */
public interface IHazelcastInstanceLoader {

    /**
     * Applies a set of properties to the factory
     *
     * @param props properties to apply
     */
    void configure(Properties props);

    /**
     * Create a new {@link com.hazelcast.core.HazelcastInstance} or loads an already
     * existing instances by it's name.
     *
     * @return new or existing HazelcastInstance (either client or node mode)
     * @throws CacheException all exceptions wrapped to CacheException
     */
    HazelcastInstance loadInstance() throws CacheException;

    /**
     * Tries to shutdown a HazelcastInstance
     *
     * @throws CacheException all exceptions wrapped to CacheException
     */
    void unloadInstance() throws CacheException;
}
