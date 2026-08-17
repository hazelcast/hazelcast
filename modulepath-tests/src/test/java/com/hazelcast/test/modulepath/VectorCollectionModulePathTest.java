/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.modulepath;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertNotNull;

@Category(ModulePathTest.class)
public class VectorCollectionModulePathTest {

    @After
    public void after() {
        HazelcastClient.shutdownAll();
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void smokeTest() {
        var collectionName = "name";
        Config config = TestUtils.createConfigWithTcpJoin(9818, 9818);
        config.addVectorCollectionConfig(new VectorCollectionConfig(collectionName).addVectorIndexConfig(
            new VectorIndexConfig().setDimension(1).setMetric(Metric.DOT)
        ));
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

        var vectorCollection = instance.getVectorCollection(collectionName);
        vectorCollection.putAsync("key", VectorDocument.of("value", VectorValues.of(new float[] {1.0f})))
            .toCompletableFuture()
            .join();

        var getResult = vectorCollection.getAsync("key").toCompletableFuture().join();
        assertNotNull("Get result is null", getResult);
    }
}
