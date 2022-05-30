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

package com.hazelcast.tpc;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.table.Table;

public class LoadMain {

    public static void main(String[] args) throws Exception {
        HazelcastInstance node1 = Hazelcast.newHazelcastInstance();


        Table table = node1.getTable("foo");

        int count = 1000 * 1000;
        int concurrency = 1;
        long start = System.currentTimeMillis();
        for (int k = 0; k < count / concurrency; k++) {
            //table.concurrentNoop(concurrency);
            table.concurrentRandomLoad("foo".getBytes(), concurrency);
        }
        long duration = System.currentTimeMillis() - start;

        System.out.println("throughput:" + (count * 1000f / duration));

        System.out.println("done");
    }
}
