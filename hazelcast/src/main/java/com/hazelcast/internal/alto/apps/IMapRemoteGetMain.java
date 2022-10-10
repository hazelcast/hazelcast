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
import com.hazelcast.map.IMap;

/**
 * A benchmark that measures performance of a remote empty get.
 */
public class IMapRemoteGetMain {

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.tpc.enabled","false");
        System.setProperty("hazelcast.alto.enabled","false");
        HazelcastInstance node1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance node2 = Hazelcast.newHazelcastInstance();

        System.out.println("Waiting for partition tables to settle");
        Thread.sleep(5000);
        System.out.println("Waiting for partition tables to settle: done");

        Integer x=0;
        for(;;) {
            if(node2.getLocalEndpoint().getSocketAddress()
                    .equals(node1.getPartitionService().getPartition(x).getOwner().getSocketAddress())){
                break;
            }
            x++;
        }
        IMap imap = node1.getMap("sometable");

        int items = 1_000_000;

        long start = System.currentTimeMillis();

        for (int k = 0; k < items; k++) {
            if (k % 100000 == 0) {
                System.out.println("Inserting at: " + k);
            }

            imap.get(x);
        }

        long duration = System.currentTimeMillis() - start;
        double throughput = items * 1000f / duration;
        System.out.println("throughput: " + throughput);
        System.out.println("Done");
    }
}
