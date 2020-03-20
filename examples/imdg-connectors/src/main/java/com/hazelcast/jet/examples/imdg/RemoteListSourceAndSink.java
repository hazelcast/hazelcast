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
import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import java.util.ArrayList;

/**
 * Demonstrates the usage of a Hazelcast IList in an external cluster
 * as source and sink with the Pipeline API. It takes the contents of
 * a list of integers, maps them to strings, and dumps the results into
 * another list.
 */
public class RemoteListSourceAndSink {

    private static final int ITEM_COUNT = 10;
    private static final String LIST_1 = "list-1";
    private static final String LIST_2 = "list-2";

    public static void main(String[] args) {
        JetInstance localJet = Jet.newJetInstance();
        try {
            HazelcastInstance externalHz = startExternalHazelcast();

            IList<Integer> sourceList = externalHz.getList(LIST_1);
            for (int i = 0; i < ITEM_COUNT; i++) {
                sourceList.add(i);
            }

            ClientConfig clientConfig = clientConfigForExternalHazelcast();

            // pipeline that copies the remote list to a local with the same name
            Pipeline p1 = Pipeline.create();
            p1.readFrom(Sources.remoteList(LIST_1, clientConfig))
              .writeTo(Sinks.list(LIST_1));

            // pipeline that copies the local list to a remote with a different name
            Pipeline p2 = Pipeline.create();
            p2.readFrom(Sources.list(LIST_1))
              .writeTo(Sinks.remoteList(LIST_2, clientConfig));

            localJet.newJob(p1).join();
            System.out.println("Local list-1 contents: " + new ArrayList<>(localJet.getList(LIST_1)));
            localJet.newJob(p2).join();
            System.out.println("Remote list-2 contents: " + new ArrayList<>(externalHz.getList(LIST_2)));
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
