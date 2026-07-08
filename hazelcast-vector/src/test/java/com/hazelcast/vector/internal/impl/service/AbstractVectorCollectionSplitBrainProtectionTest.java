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

package com.hazelcast.vector.internal.impl.service;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Pipelining;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.splitbrainprotection.AbstractSplitBrainProtectionTest;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.internal.impl.VectorTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.concurrent.ExecutionException;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public abstract class AbstractVectorCollectionSplitBrainProtectionTest extends AbstractSplitBrainProtectionTest {

    @Parameterized.Parameter
    public static SplitBrainProtectionOn splitBrainProtectionOn;

    @BeforeClass
    public static void setUp() {
        initTestEnvironment(smallInstanceConfig(), new TestHazelcastInstanceFactory(),
                AbstractVectorCollectionSplitBrainProtectionTest::initVectorCollectionData);
    }

    protected static void initVectorCollectionData(SplitBrainProtectionOn... types)
            throws ExecutionException, InterruptedException {
        for (SplitBrainProtectionOn splitBrainProtectionOn : types) {
            populateVectorCollection(vectorCollection(cluster.instance[0], splitBrainProtectionOn));
        }
    }

    protected static void populateVectorCollection(VectorCollection<String, String> collection)
            throws InterruptedException, ExecutionException {
        Pipelining<Void> pipelining = new Pipelining<>(24);
        for (int i = 0; i < 100; i++) {
            pipelining.add(collection.setAsync("" + i, VectorDocument.of("" + i, VectorTestUtils.randomVec(2))));
        }
        pipelining.results();
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
    }

    protected VectorCollection<String, String> vectorCollection(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return vectorCollection(cluster.instance[index], splitBrainProtectionOn);
    }

    private static VectorCollection<String, String> vectorCollection(HazelcastInstance instance, SplitBrainProtectionOn splitBrainProtectionOn) {
        return instance.getVectorCollection(VECTOR_COLLECTION_NAME + splitBrainProtectionOn.name());
    }
}
