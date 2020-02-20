/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.examples.eventjournal;

import com.hazelcast.cache.ICache;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;

/**
 * A pipeline which streams events from an ICache on a remote Hazelcast
 * cluster. The values for new entries are extracted and then
 * written to a local IList in the Jet cluster.
 */
public class RemoteCacheJournalSource {

    private static final String CACHE_NAME = "cache";
    private static final String SINK_NAME = "list";

    public static void main(String[] args) throws Exception {
        Config hzConfig = getConfig();
        HazelcastInstance remoteHz = startRemoteHzCluster(hzConfig);
        JetInstance localJet = startLocalJetCluster();

        try {
            ClientConfig clientConfig = new ClientConfig();

            clientConfig.getNetworkConfig().addAddress(getAddress(remoteHz));
            clientConfig.setClusterName(hzConfig.getClusterName());

            Pipeline p = Pipeline.create();
            p.readFrom(Sources.<Integer, Integer>remoteCacheJournal(
                    CACHE_NAME, clientConfig, START_FROM_OLDEST)
            ).withoutTimestamps()
             .map(Entry::getValue)
             .writeTo(Sinks.list(SINK_NAME));

            localJet.newJob(p);

            ICache<Integer, Integer> cache = remoteHz.getCacheManager().getCache(CACHE_NAME);
            for (int i = 0; i < 1000; i++) {
                cache.put(i, i);
            }

            TimeUnit.SECONDS.sleep(3);
            System.out.println("Read " + localJet.getList(SINK_NAME).size() + " entries from remote cache journal.");
        } finally {
            Hazelcast.shutdownAll();
            Jet.shutdownAll();
        }

    }

    private static String getAddress(HazelcastInstance remoteHz) {
        Address address = remoteHz.getCluster().getLocalMember().getAddress();
        return address.getHost() + ":" + address.getPort();
    }

    private static JetInstance startLocalJetCluster() {
        JetInstance localJet = Jet.newJetInstance();
        Jet.newJetInstance();
        return localJet;
    }

    private static HazelcastInstance startRemoteHzCluster(Config config) {
        HazelcastInstance remoteHz = Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);
        return remoteHz;
    }

    private static Config getConfig() {
        Config config = new Config();
        config.addCacheConfig(new CacheSimpleConfig().setName(CACHE_NAME));
        // Add an event journal config for cache which has custom capacity of 10_000
        // and time to live seconds as 10 seconds (default 0 which means infinite)
        config.getCacheConfig(CACHE_NAME)
              .getEventJournalConfig()
              .setEnabled(true)
              .setCapacity(10_000)
              .setTimeToLiveSeconds(10);
        return config;
    }

}
