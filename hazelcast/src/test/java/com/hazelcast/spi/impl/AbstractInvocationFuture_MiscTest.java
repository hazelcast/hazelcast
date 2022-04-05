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

package com.hazelcast.spi.impl;

import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AbstractInvocationFuture_MiscTest extends AbstractInvocationFuture_AbstractTest {

    @Test
    public void toString_whenNotCompleted() {
        String s = future.toString();
        assertEquals("InvocationFuture{invocation=someinvocation, done=false}", s);
    }

    @Test
    public void toString_whenCompleted() {
        future.complete(value);
        String s = future.toString();
        assertEquals("InvocationFuture{invocation=someinvocation, value=somevalue}", s);
    }

    @Test
    public void getNumberOfDependents_whenNoneRegistered() {
        assertEquals(0, future.getNumberOfDependents());
    }

    @Test
    public void getNumberOfDependents_whenSomeRegistered() {
        future.whenCompleteAsync((v, t) -> ignore(null));
        future.thenRun(() -> ignore(null));
        assertEquals(2, future.getNumberOfDependents());
    }

    @Test
    public void getNumberOfDependents_whenOneIsRegistered() {
        future.thenRun(() -> ignore(null));
        assertEquals(1, future.getNumberOfDependents());
    }

    @Test
    public void obtrudeValue_whenIncomplete() {
        future.obtrudeValue(value);

        assertSame(value, future.join());
    }

    @Test
    public void obtrudeValue_whenComplete() {
        future.complete("this will be overwritten");
        future.obtrudeValue(value);

        assertSame(value, future.join());
    }

    @Test
    public void obtrudeValue_triggersUntriggeredStages() {
        InternalCompletableFuture nextStage = future.thenAccept(v -> assertSame(value, v));
        future.obtrudeValue(value);

        assertSame(value, future.join());
        // ensure the consumer was executed and no exception was thrown
        nextStage.join();
    }

    @Test
    public void obtrudeException_whenNotComplete() {
        future.obtrudeException(new ExpectedRuntimeException());

        try {
            future.joinInternal();
            fail();
        } catch (ExpectedRuntimeException e) {
            ignore(e);
        }
    }

    @Test
    public void obtrudeException_whenComplete() {
        future.complete(value);
        future.obtrudeException(new ExpectedRuntimeException());

        try {
            future.joinInternal();
            fail();
        } catch (ExpectedRuntimeException e) {
            ignore(e);
        }
    }
}
