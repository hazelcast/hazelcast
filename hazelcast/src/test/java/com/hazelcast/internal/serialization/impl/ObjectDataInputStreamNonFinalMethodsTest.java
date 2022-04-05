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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.util.Random;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ObjectDataInputStreamNonFinalMethodsTest {

    static final byte[] INIT_DATA = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    private ObjectDataInputStream in;
    private TestInputStream inputStream;

    @Before
    public void before() throws Exception {
        InternalSerializationService mockSerializationService = mock(InternalSerializationService.class);
        when(mockSerializationService.getByteOrder()).thenReturn(BIG_ENDIAN);

        inputStream = spy(new TestInputStream(INIT_DATA));
        in = new ObjectDataInputStream(inputStream, mockSerializationService);
    }

    @Test
    public void testSkip() throws Exception {
        long someInput = new Random().nextLong();
        in.skip(someInput);
        verify(inputStream).skip(someInput);
    }

    @Test
    public void testAvailable() throws Exception {
        in.available();
        verify(inputStream).available();
    }

    @Test
    public void testClose() throws Exception {
        in.close();
        verify(inputStream).close();
    }

    @Test
    public void testMark() {
        int someInput = new Random().nextInt();
        in.mark(someInput);
        verify(inputStream).mark(someInput);
    }

    @Test
    public void testReset() throws Exception {
        in.reset();
        verify(inputStream).reset();
    }

    @Test
    public void testMarkSupported() {
        in.markSupported();
        verify(inputStream).markSupported();
    }

    static class TestInputStream extends ByteArrayInputStream {
        TestInputStream(byte[] buf) {
            super(buf);
        }
    }
}
