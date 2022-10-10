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

public class TableGetBenchmark {

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.alto.enabled","true");
        System.setProperty("hazelcast.tpc.reactor.count","1");
        HazelcastInstance node1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance node2 = Hazelcast.newHazelcastInstance();

        Table table = node1.getTable("sometable");

        int items = 1_000_000;

        for (int k = 0; k < items; k++) {
            if (k % 100000 == 0) {
                System.out.println("Inserting at: " + k);
            }

            byte[] key = ("" + k).getBytes();
            byte[] value = ("value-" + k).getBytes();
            //byte[] value = new byte[1024];
            table.set(key, value);
        }
//
        long start = System.currentTimeMillis();
        int queryCount = 2000;
        for (int k = 0; k < queryCount; k++) {
            if (k % 1000 == 0) {
                System.out.println("Getting at: " + k);
            }

            String key = "" + k;
            byte[] value = table.get(key.getBytes());
            String actual = new String(value);
            String expected = "value-" + k;
            if (!expected.equals(actual)) {
                throw new RuntimeException("Expected " + expected + " but found: " + actual);
            }
        }

        long duration = System.currentTimeMillis() - start;
        double throughput = queryCount * 1000f / duration;
        node1.shutdown();
        System.out.println("throughput: " + throughput);
        System.out.println("Done");
    }
}
