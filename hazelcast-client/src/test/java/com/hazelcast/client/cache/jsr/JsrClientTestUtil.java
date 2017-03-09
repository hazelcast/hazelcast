/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.cache.jsr;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;

import javax.cache.Cache;
import javax.cache.Caching;

/**
 * utility class responsible for setup/cleanup client jsr tests
 */
public class JsrClientTestUtil {

    static {
        System.setProperty("javax.management.builder.initial", "com.hazelcast.cache.impl.TCKMBeanServerBuilder");
        System.setProperty("org.jsr107.tck.management.agentId", "TCKMbeanServer");
        System.setProperty(Cache.class.getName(), "com.hazelcast.cache.ICache");
        System.setProperty(Cache.Entry.class.getCanonicalName(), "com.hazelcast.cache.impl.CacheEntry");
    }

    public static void setup() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        Hazelcast.newHazelcastInstance(config);
    }

    public static void cleanup() {
        Caching.getCachingProvider().close();
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }
}
