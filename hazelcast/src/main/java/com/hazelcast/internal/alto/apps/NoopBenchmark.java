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

package com.hazelcast.internal.alto.apps;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.table.Table;

public class NoopBenchmark {
    public static long operations = 5_000_000;
    public static int concurrency = 2;

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.alto.enabled", "true");
        System.setProperty("hazelcast.tpc.reactor.count", "1");

        HazelcastInstance node1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance node2 = Hazelcast.newHazelcastInstance();
       // HazelcastInstance node2 = Hazelcast.newHazelcastInstance();

        Table table = node1.getTable("sometable");

        long iterations = operations / concurrency;

        long startMs = System.currentTimeMillis();
        long count = 0;
        for (int k = 0; k < iterations; k++) {

            if (count % 1_000_000 == 0) {
                System.out.println("at k:" + count);
            }

            table.concurrentNoop(concurrency);
            count += concurrency;
        }

        System.out.println("Done");

        long duration = System.currentTimeMillis() - startMs;
        System.out.println("Throughput: " + (operations * 1000.0f / duration) + " op/s");
        node1.shutdown();
        //remoteNode.shutdown();
        System.exit(0);
    }
}
