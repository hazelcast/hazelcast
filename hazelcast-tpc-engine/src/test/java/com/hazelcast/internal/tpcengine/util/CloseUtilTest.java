/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.util;

import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeAllQuietly;
import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeQuietly;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class CloseUtilTest {

    @Test
    public void test_closeQuietly_whenNull() {
        closeQuietly(null);
    }

    @Test
    public void test_closeQuietly_whenNoException() throws Exception {
        AutoCloseable closeable = mock(Closeable.class);
        closeQuietly(closeable);
        verify(closeable).close();
    }

    @Test
    public void test_closeResource_whenException() throws Exception {
        Closeable closeable = mock(Closeable.class);
        doThrow(new IOException("expected")).when(closeable).close();

        closeQuietly(closeable);

        verify(closeable).close();
        verifyNoMoreInteractions(closeable);
    }

    @Test
    public void test_closeQuietly_whenException() throws Exception {
        Closeable closeable = mock(Closeable.class);
        doThrow(new IOException("expected")).when(closeable).close();

        closeQuietly(closeable);

        verify(closeable).close();
        verifyNoMoreInteractions(closeable);
    }

    @Test
    public void test_closeAllQuietly_whenNullCollection() {
        closeAllQuietly(null);
    }

    @Test
    public void test_closeAllQuietly_whenItemNullCollection() throws IOException {
        Closeable closeable1 = mock(Closeable.class);
        Closeable closeable2 = mock(Closeable.class);

        List<Closeable> closeableList = new ArrayList<>();
        closeableList.add(closeable1);
        closeableList.add(null);
        closeableList.add(closeable2);

        closeAllQuietly(closeableList);

        verify(closeable1).close();
        verify(closeable2).close();
    }
}
