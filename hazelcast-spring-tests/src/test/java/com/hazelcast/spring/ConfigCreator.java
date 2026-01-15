/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.spring;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.RingbufferConfig;

public final class ConfigCreator {
    private ConfigCreator() {
    }

    public static Config createConfig() {
        var config = new Config();
        config.getJetConfig().setEnabled(true);

        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.setPort(0);
        networkConfig.getInterfaces().addInterface("127.0.0.1");
        networkConfig.getJoin().getAutoDetectionConfig().setEnabled(false);
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);

        config.setProperty("hazelcast.merge.first.run.delay.seconds", "5");
        config.setProperty("hazelcast.merge.next.run.delay.seconds", "5");
        config.setProperty("hazelcast.partition.count", "277");
        return config;
    }

    public static Config createConfigWithStructures() {
        Config config = ConfigCreator.createConfig();
        config.setClusterName("spring-hazelcast-cluster-from-java");

        var mapConfig = new MapConfig("testMap");
        mapConfig.setBackupCount(2);
        mapConfig.setReadBackupData(true);
        config.addMapConfig(mapConfig);
        var map1Config = new MapConfig("map1");
        map1Config.setBackupCount(2);
        map1Config.setReadBackupData(true);
        config.addMapConfig(map1Config);

        var ringbufferConfig = new RingbufferConfig("ringbuffer");
        config.addRingBufferConfig(ringbufferConfig);

        return config;
    }
}
