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

package com.hazelcast.spi.impl.proxyservice.impl;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.spi.impl.InitializingObject;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DistributedObjectFutureTest {

    private DistributedObject object = mock(InitializingDistributedObject.class);
    private DistributedObjectFuture future = new DistributedObjectFuture(UuidUtil.newUnsecureUUID());

    @Test
    public void isSet_returnsFalse_whenNotSet() {
        assertFalse(future.isSetAndInitialized());
    }

    @Test
    public void isSet_returnsTrue_whenSet() {
        future.set(object, true);
        assertTrue(future.isSetAndInitialized());
    }

    @Test
    public void isSet_returnsFalse_whenSetUninitialized() {
        future.set(object, false);
        assertFalse(future.isSetAndInitialized());
    }

    @Test
    public void isSet_returnsTrue_whenErrorSet() {
        future.setError(new Throwable());
        assertTrue(future.isSetAndInitialized());
    }

    @Test
    public void get_returnsObject_whenObjectSet() {
        future.set(object, true);
        assertSame(object, future.get());

        InitializingObject initializingObject = (InitializingObject) object;
        verify(initializingObject, never()).initialize();
    }

    @Test
    public void get_returnsInitializedObject_whenUninitializedObjectSet() {
        future.set(object, false);
        assertSame(object, future.get());

        InitializingObject initializingObject = (InitializingObject) object;
        verify(initializingObject).initialize();
    }

    @Test
    public void get_throwsGivenException_whenUncheckedExceptionSet() {
        Throwable error = new RuntimeException();
        future.setError(error);

        try {
            future.get();
        } catch (Exception e) {
            assertSame(error, e);
        }
    }

    @Test
    public void get_throwsWrappedException_whenCheckedExceptionSet() {
        Throwable error = new Throwable();
        future.setError(error);

        try {
            future.get();
        } catch (Exception e) {
            assertSame(error, e.getCause());
        }
    }

    private interface InitializingDistributedObject extends DistributedObject, InitializingObject {
    }
}
