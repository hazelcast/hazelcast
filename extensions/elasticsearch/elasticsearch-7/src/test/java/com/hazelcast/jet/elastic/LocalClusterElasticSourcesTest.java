/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.elastic;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;

import com.hazelcast.jet.impl.util.Util;
import org.junit.AfterClass;

import java.util.function.Supplier;

/**
 * Test running 3 local Jet members in a cluster and Elastic in docker
 */
public class LocalClusterElasticSourcesTest extends CommonElasticSourcesTest {

    private static HazelcastInstance[] instances;

    // Cluster startup takes >1s, reusing the cluster between tests
    private static Supplier<HazelcastInstance> hzSupplier = Util.memoize(() -> {
        TestHazelcastFactory factory = new TestHazelcastFactory();
        instances = factory.newInstances(config(), 3);
        return instances[0];
    });


    @AfterClass
    public static void afterClass() {
        if (instances != null) {
            for (HazelcastInstance instance : instances) {
                instance.shutdown();
            }
            instances = null;
        }
    }

    @Override
    protected HazelcastInstance createHazelcastInstance() {
        return hzSupplier.get();
    }
}
