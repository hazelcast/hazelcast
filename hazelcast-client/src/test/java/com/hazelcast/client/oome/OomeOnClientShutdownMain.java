/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.oome;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * A test application that starts and stops clients. There was a  bug in the system that eventually leads to
 * an OOME and this application was used to detect that bug. So run this application e.g. in combination with a profiler
 * and see how memory usage behaves.
 */
public class OomeOnClientShutdownMain {

    public static void main(String[] args) {
        Hazelcast.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();

        for (int k = 0; k < 1000000; k++) {
            System.out.println("At:" + k);
            HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
            client.shutdown();
        }
    }
}
