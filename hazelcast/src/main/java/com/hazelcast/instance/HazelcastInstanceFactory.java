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

package com.hazelcast.instance;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;

/***
 * This is interface which provides capability for Hazelcast factories customization;
 * DefaultHazelcastInstanceFactory is used by default, but external service can have;
 * custom implementation which should be specified in META-INF services;
 */
public interface HazelcastInstanceFactory {
    HazelcastInstance newHazelcastInstance(Config config, String instanceName,
                                           NodeContext nodeContext) throws Exception;

    HazelcastInstance newHazelcastInstance(Config config) throws Exception;
}
