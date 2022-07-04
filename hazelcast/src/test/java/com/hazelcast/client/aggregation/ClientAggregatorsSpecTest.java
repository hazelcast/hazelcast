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

package com.hazelcast.client.aggregation;

import com.hazelcast.aggregation.AggregatorsSpecTest;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import static com.hazelcast.spi.properties.ClusterProperty.AGGREGATION_ACCUMULATION_PARALLEL_EVALUATION;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientAggregatorsSpecTest extends AggregatorsSpecTest {

    private TestHazelcastFactory factory;

    @Override
    protected <K, V> IMap<K, V> getMapWithNodeCount(int nodeCount, boolean parallelAccumulation, boolean useIndex) {
        if (nodeCount < 1) {
            throw new IllegalArgumentException("node count < 1");
        }

        MapConfig mapConfig = new MapConfig()
                .setName("aggr")
                .setInMemoryFormat(inMemoryFormat);

        if (useIndex) {
            mapConfig.addIndexConfig(new IndexConfig(IndexConfig.DEFAULT_TYPE, "fieldWeCanQuery"));
        }

        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), String.valueOf(nodeCount))
                .setProperty(AGGREGATION_ACCUMULATION_PARALLEL_EVALUATION.getName(), String.valueOf(parallelAccumulation))
                .addMapConfig(mapConfig);

        factory = new TestHazelcastFactory();
        for (int i = 0; i < nodeCount; i++) {
            factory.newHazelcastInstance(config);
        }
        HazelcastInstance instance = factory.newHazelcastClient();
        return instance.getMap("aggr");
    }

    @After
    public void teardown() {
        factory.terminateAll();
    }
}
