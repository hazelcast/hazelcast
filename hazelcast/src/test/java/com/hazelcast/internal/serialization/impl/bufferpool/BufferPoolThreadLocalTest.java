/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.bufferpool;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.ref.WeakReference;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BufferPoolThreadLocalTest extends HazelcastTestSupport {

    private InternalSerializationService serializationService;
    private BufferPoolThreadLocal bufferPoolThreadLocal;

    @Before
    public void setup() {
        serializationService = mock(InternalSerializationService.class);
        bufferPoolThreadLocal = new BufferPoolThreadLocal(serializationService, new BufferPoolFactoryImpl()
                , HazelcastInstanceNotActiveException::new);
    }

    @Test
    public void get_whenSameThread_samePoolInstance() {
        BufferPool pool1 = bufferPoolThreadLocal.get();
        BufferPool pool2 = bufferPoolThreadLocal.get();
        assertSame(pool1, pool2);
    }

    @Test
    public void get_whenDifferentThreads_thenDifferentInstances() throws Exception {
        BufferPool pool1 = bufferPoolThreadLocal.get();
        BufferPool pool2 = spawn(() -> bufferPoolThreadLocal.get()).get();

        assertNotSame(pool1, pool2);
    }

    @Test
    public void get_whenCleared() {
        // forces the creation of a bufferpool.
        bufferPoolThreadLocal.get();

        // we kill all strong references.
        bufferPoolThreadLocal.clear();

        // then eventually when we try to get the pool, we should get a HazelcastInstanceNotActiveException
        assertTrueEventually(() -> {
            System.gc();
            try {
                bufferPoolThreadLocal.get();
                fail();
            } catch (HazelcastInstanceNotActiveException ignore) {
            }
        });
    }


    // we need to make sure that different instances return different bufferpool (each hz
    // instance should gets its own bufferpool).
    @Test
    public void get_whenDifferentThreadLocals_thenDifferentInstances() {
        BufferPoolThreadLocal bufferPoolThreadLocal2 = new BufferPoolThreadLocal(
                serializationService, new BufferPoolFactoryImpl(), null);

        BufferPool pool1 = bufferPoolThreadLocal.get();
        BufferPool pool2 = bufferPoolThreadLocal2.get();

        assertNotSame(pool1, pool2);
    }

    // if clear is called, all strong references to the pool are cut, and therefor eventually the buffer-pool
    //should be gc'd
    @Test
    public void clear() {
        // store the pool in a weak reference since we don't want to force a strong reference ourselves.
        final WeakReference<BufferPool> poolRef = new WeakReference<>(bufferPoolThreadLocal.get());

        // call clear; kills the strong references.
        bufferPoolThreadLocal.clear();

        assertTrueEventually(() -> {
            System.gc();
            // eventually the reference should point to zero; indicating that the pool is gc'ed.
            assertNull(poolRef.get());
        });
    }
}
