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

import com.hazelcast.client.splitbrainprotection.PartitionedClusterClients;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import com.hazelcast.vector.VectorCollection;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ClientVectorCollectionSplitBrainProtectionReadTest extends VectorCollectionSplitBrainProtectionReadTest {

    private static PartitionedClusterClients clients;

    @BeforeClass
    public static void setUp() {
        TestHazelcastFactory factory = new TestHazelcastFactory();
        initTestEnvironment(smallInstanceConfig(), factory,
                AbstractVectorCollectionSplitBrainProtectionTest::initVectorCollectionData);
        clients = new PartitionedClusterClients(cluster, factory);
    }

    @AfterClass
    public static void tearDown() {
        if (clients != null) {
            clients.terminateAll();
        }
        clients = null;

        shutdownTestEnvironment();
    }

    @Override
    protected VectorCollection<String, String> vectorCollection(int index, SplitBrainProtectionOn splitBrainProtectionOn) {
        return clients.client(index).getVectorCollection(VECTOR_COLLECTION_NAME + splitBrainProtectionOn.name());
    }
}
