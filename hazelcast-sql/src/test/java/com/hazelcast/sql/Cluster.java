/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.sql.pojos.Person;

public class Cluster {

    private static final int SIZE = 3;

    public static void main(String[] args) {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("localhost");
        config.getNetworkConfig().setPort(10000);

        for (int i = 0; i < SIZE; ++i) {
            HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

            if (i == SIZE - 1) {
                IMap<Integer, Person> persons = instance.getMap("persons");
                for (int j = 0; j < 10; ++j) {
                    persons.put(j, new Person(j));
                }
            }
        }

    }

}
