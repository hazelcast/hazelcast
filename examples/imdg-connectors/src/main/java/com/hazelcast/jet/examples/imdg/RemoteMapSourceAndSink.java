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

package com.hazelcast.jet.examples.imdg;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;

/**
 * Demonstrates the usage of Hazelcast IMap as source and sink
 * from/to a remote Hazelcast cluster.
 */
public class RemoteMapSourceAndSink {

    private static final String MAP_1 = "map-1";
    private static final String MAP_2 = "map-2";
    private static final int ITEM_COUNT = 10;

    public static void main(String[] args) {
        JetInstance localJet = Jet.newJetInstance();
        try {
            HazelcastInstance externalHz = startExternalHazelcast();

            IMap<Integer, Integer> sourceMap = externalHz.getMap(MAP_1);
            for (int i = 0; i < ITEM_COUNT; i++) {
                sourceMap.put(i, i);
            }

            ClientConfig clientConfig = clientConfigForExternalHazelcast();

            // pipeline that copies the remote map to a local with the same name
            Pipeline p1 = Pipeline.create();
            p1.readFrom(Sources.remoteMap(MAP_1, clientConfig))
              .writeTo(Sinks.map(MAP_1));

            // pipeline that copies the local map to a remote with different name
            Pipeline p2 = Pipeline.create();
            p2.readFrom(Sources.map(MAP_1))
              .writeTo(Sinks.remoteMap(MAP_2, clientConfig));

            localJet.newJob(p1).join();
            System.out.println("Local map-1 contents: " + localJet.getMap(MAP_1).entrySet());
            localJet.newJob(p2).join();
            System.out.println("Remote map-2 contents: " + externalHz.getMap(MAP_2).entrySet());
        } finally {
            Jet.shutdownAll();
            Hazelcast.shutdownAll();
        }
    }

    private static HazelcastInstance startExternalHazelcast() {
        System.out.println("Creating and populating remote Hazelcast instance...");
        Config config = new Config();
        config.getNetworkConfig().setPort(6701);
        return Hazelcast.newHazelcastInstance(config);
    }

    private static ClientConfig clientConfigForExternalHazelcast() {
        ClientConfig cfg = new ClientConfig();
        cfg.getNetworkConfig().addAddress("localhost:6701");
        cfg.setClusterName("dev");
        return cfg;
    }
}
