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

package com.hazelcast.client.mapreduce;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.IMap;

@SuppressWarnings("unused")
public abstract class AbstractClientMapReduceJobTest {

    protected ClientConfig buildClientConfig() {
        return new XmlClientConfigBuilder().build();
    }

    protected Config buildConfig() {
        Config config = new XmlConfigBuilder().build();
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        return config;
    }

    protected void populateMap(IMap<Integer, Integer> map, int count) {
        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }
    }

    protected int calculateExceptedResult(int count) {
        int expectedResult = 0;
        for (int i = 0; i < count; i++) {
            expectedResult += i;
        }
        return expectedResult;
    }

    protected int[] calculateExpectedResult(int arraySize, int count) {
        int[] expectedResults = new int[arraySize];
        for (int i = 0; i < count; i++) {
            int index = i % arraySize;
            expectedResults[index] += i;
        }
        return expectedResults;
    }
}
