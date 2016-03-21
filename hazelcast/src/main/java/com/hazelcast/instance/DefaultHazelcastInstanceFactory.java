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
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;

/***
 * Default implementation for Hazelcast instance factory;
 */
public class DefaultHazelcastInstanceFactory implements HazelcastInstanceFactory {
    protected Config wrapConfig(Config config) {
        if (config == null) {
            config = new XmlConfigBuilder().build();
        }

        return config;
    }

    @Override
    public HazelcastInstance newHazelcastInstance(Config config, String instanceName,
                                                  NodeContext nodeContext) throws Exception {
        return new HazelcastInstanceImpl(instanceName, wrapConfig(config), nodeContext);
    }

    @Override
    public HazelcastInstance newHazelcastInstance(Config config) throws Exception {
        Config wrappedConfig = wrapConfig(config);
        return newHazelcastInstance(wrappedConfig, wrappedConfig.getInstanceName(), new DefaultNodeContext());
    }
}
