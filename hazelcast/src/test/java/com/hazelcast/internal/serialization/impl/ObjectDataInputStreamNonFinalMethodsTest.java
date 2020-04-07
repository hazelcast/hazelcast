/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.util.Random;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ObjectDataInputStreamNonFinalMethodsTest {

    static final byte[] INIT_DATA = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    private InternalSerializationService mockSerializationService;
    private ObjectDataInputStream in;
    private DataInputStream dataInputSpy;
    private ByteArrayInputStream inputStream;
    private ByteOrder byteOrder;

    @Before
    public void before() throws Exception {
        byteOrder = BIG_ENDIAN;
        mockSerializationService = mock(InternalSerializationService.class);
        when(mockSerializationService.getByteOrder()).thenReturn(byteOrder);

        inputStream = new ByteArrayInputStream(INIT_DATA);
        in = new ObjectDataInputStream(inputStream, mockSerializationService);

        Field field = ObjectDataInputStream.class.getDeclaredField("dataInput");
        field.setAccessible(true);
        DataInputStream dataInput = (DataInputStream) field.get(in);

        dataInputSpy = spy(dataInput);

        field.set(in, dataInputSpy);
    }

    @Test
    public void testSkip() throws Exception {
        long someInput = new Random().nextLong();
        in.skip(someInput);
        verify(dataInputSpy).skip(someInput);
    }

    @Test
    public void testAvailable() throws Exception {
        in.available();
        verify(dataInputSpy).available();
    }

    @Test
    public void testClose() throws Exception {
        in.close();
        verify(dataInputSpy).close();
    }

    @Test
    public void testMark() throws Exception {
        int someInput = new Random().nextInt();
        in.mark(someInput);
        verify(dataInputSpy).mark(someInput);
    }

    @Test
    public void testReset() throws Exception {
        in.reset();
        verify(dataInputSpy).reset();
    }

    @Test
    public void testMarkSupported() throws Exception {
        in.markSupported();
        verify(dataInputSpy).markSupported();
    }
}
