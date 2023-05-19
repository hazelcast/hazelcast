/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpc.apps;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.noop.Noop;
import com.hazelcast.spi.properties.ClusterProperty;

@SuppressWarnings({"checkstyle:MagicNumber", "checkstyle:VisibilityModifier", "checkstyle:HideUtilityClassConstructor"})
public class NoopBenchmark {
    public static long operations = 5_000_000;
    public static int concurrency = 2;

    public static void main(String[] args) throws Exception {
        System.setProperty(ClusterProperty.TPC_ENABLED.getName(), "true");
        System.setProperty(ClusterProperty.TPC_EVENTLOOP_COUNT.getName(), "1");

        HazelcastInstance node1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance node2 = Hazelcast.newHazelcastInstance();
        // HazelcastInstance node2 = Hazelcast.newHazelcastInstance();

        Noop nop = node1.getProxy(Noop.class, "sometable");

        long iterations = operations / concurrency;

        long startMs = System.currentTimeMillis();
        long count = 0;
        for (int k = 0; k < iterations; k++) {

            if (count % 1_000_000 == 0) {
                System.out.println("at k:" + count);
            }

            nop.concurrentNoop(concurrency, 0);
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
