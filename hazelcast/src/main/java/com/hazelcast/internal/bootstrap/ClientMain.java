/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.bootstrap;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

/**
 * Demo application for TPC. Will be removed in in the final release.
 */
@SuppressWarnings("all")
public class ClientMain {

    public static void main(String[] args) {
        System.setProperty("hazelcast.tpc.enabled", "true");
        System.setProperty("hazelcast.tpc.eventloop.count", "" + Runtime.getRuntime().availableProcessors());
        HazelcastInstance server = Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        System.out.println("Client created");
        IMap<Integer, Integer> map = client.getMap("foo");

        long count = 4_000_000;
        long startTime = System.currentTimeMillis();

        for (int k = 0; k < count; k++) {
            if (k % 100000 == 0) {
                System.out.println("At:" + k);
            }
            map.put(k, k);
        }

        long duration = System.currentTimeMillis() - startTime;
        double throughput = count * 1000f / duration;
        System.out.println("Throughput:" + throughput + " op/s");
        System.exit(0);
    }
}
