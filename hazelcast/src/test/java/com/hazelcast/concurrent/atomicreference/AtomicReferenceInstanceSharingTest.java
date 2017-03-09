/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AtomicReferenceInstanceSharingTest extends HazelcastTestSupport {

    private HazelcastInstance[] instances;
    private HazelcastInstance local;
    private HazelcastInstance remote;

    @Before
    public void setUp() {
        instances = createHazelcastInstanceFactory(2).newInstances();
        warmUpPartitions(instances);
        local = instances[0];
        remote = instances[1];
    }

    @Test
    public void invocationToLocalMember() throws ExecutionException, InterruptedException {
        String localKey = generateKeyOwnedBy(local);
        IAtomicReference<DummyObject> ref = local.getAtomicReference(localKey);

        DummyObject inserted = new DummyObject();
        ref.set(inserted);

        DummyObject get1 = ref.get();
        DummyObject get2 = ref.get();

        assertNotNull(get1);
        assertNotNull(get2);
        assertNotSame(get1, get2);
        assertNotSame(get1, inserted);
        assertNotSame(get2, inserted);
    }

    public static class DummyObject implements Serializable {
    }

    @Test
    public void invocationToRemoteMember() throws ExecutionException, InterruptedException {
        String localKey = generateKeyOwnedBy(remote);
        IAtomicReference<DummyObject> ref = local.getAtomicReference(localKey);

        DummyObject inserted = new DummyObject();
        ref.set(inserted);

        DummyObject get1 = ref.get();
        DummyObject get2 = ref.get();

        assertNotNull(get1);
        assertNotNull(get2);
        assertNotSame(get1, get2);
        assertNotSame(get1, inserted);
        assertNotSame(get2, inserted);
    }
}
