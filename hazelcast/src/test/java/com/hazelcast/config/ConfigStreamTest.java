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

package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

import static java.lang.Math.min;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConfigStreamTest {
    private static final byte[] TEST_BYTES = "test-bytes".getBytes();

    @Parameters(name = "readLimit={0}, expectedRead={1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {TEST_BYTES.length, "test-bytes"},
                {ConfigStream.DEFAULT_READ_LIMIT_4K, "test-bytes"},
                {4, "test"}
        });
    }

    @Parameter(0)
    public int readLimit;
    @Parameter(1)
    public String expectedRead;

    @Test
    public void resetableIsReused() throws IOException {
        InputStream mockIs = givenMockedInputStream();

        ConfigStream configStream = new ConfigStream(mockIs);
        verify(mockIs).reset();

        reset(mockIs);
        stubReadByteArr(mockIs);

        byte[] actualBytes = new byte[expectedRead.getBytes().length];
        configStream.read(actualBytes);
        verify(mockIs).read(any(byte[].class));
        assertArrayEquals(expectedRead.getBytes(), actualBytes);
    }

    @Test
    public void nonResetableIsCopied() throws IOException {
        InputStream mockIs = givenMockedInputStream();
        doThrow(IOException.class).when(mockIs).reset();

        ConfigStream configStream = new ConfigStream(mockIs);
        verify(mockIs).reset();

        reset(mockIs);

        byte[] actualBytes = new byte[expectedRead.getBytes().length];
        configStream.read(actualBytes);
        verifyZeroInteractions(mockIs);
        assertArrayEquals(expectedRead.getBytes(), actualBytes);
    }

    @Test
    public void markableIsReused() throws IOException {
        InputStream mockIs = givenMarkableMockedInputStream();

        ConfigStream configStream = new ConfigStream(mockIs);
        verify(mockIs).markSupported();

        reset(mockIs);
        stubReadByteArr(mockIs);

        byte[] actualBytes = new byte[expectedRead.getBytes().length];
        configStream.read(actualBytes);
        verify(mockIs).read(any(byte[].class));
        assertArrayEquals(expectedRead.getBytes(), actualBytes);
    }

    @Test
    public void nonMarkableIsCopied() throws IOException {
        InputStream mockIs = givenNonMarkableMockedInputStream();

        ConfigStream configStream = new ConfigStream(mockIs);
        verify(mockIs).markSupported();

        reset(mockIs);

        byte[] actualBytes = new byte[expectedRead.getBytes().length];
        configStream.read(actualBytes);
        verifyZeroInteractions(mockIs);
        assertArrayEquals(expectedRead.getBytes(), actualBytes);
    }

    private InputStream givenMockedInputStream() throws IOException {
        return givenMockedInputStream(true);
    }

    private InputStream givenMarkableMockedInputStream() throws IOException {
        return givenMockedInputStream(true);
    }

    private InputStream givenNonMarkableMockedInputStream() throws IOException {
        return givenMockedInputStream(false);
    }

    private InputStream givenMockedInputStream(boolean markable) throws IOException {
        InputStream mockIs = mock(InputStream.class);
        when(mockIs.markSupported()).thenReturn(markable);
        stubReadByteArr(mockIs);
        return mockIs;
    }

    private void stubReadByteArr(InputStream mockIs) throws IOException {
        // copy the TEST_BYTES to the byte[] argument
        when(mockIs.read(any(byte[].class))).thenAnswer(invocation -> {
            byte[] targetArr = invocation.getArgument(0);
            System.arraycopy(TEST_BYTES, 0, targetArr, 0, targetArr.length);
            return targetArr.length;
        });
        // copy the TEST_BYTES to the byte[] argument with the limits taken into account
        when(mockIs.read(any(byte[].class), anyInt(), anyInt())).thenAnswer(invocation -> {
            byte[] targetArr = invocation.getArgument(0);
            int src = invocation.getArgument(1);
            int len = invocation.getArgument(2);
            System.arraycopy(TEST_BYTES, src, targetArr, src, min(len, TEST_BYTES.length - src));
            return targetArr.length;
        });
    }

}
