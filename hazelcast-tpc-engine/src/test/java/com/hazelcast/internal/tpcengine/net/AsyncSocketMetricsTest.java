/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.net;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AsyncSocketMetricsTest {

    @Test
    public void test_bytesRead() {
        AsyncSocketMetrics metrics = new AsyncSocketMetrics();

        metrics.incBytesRead(10);
        assertEquals(10, metrics.bytesRead());

        metrics.incBytesRead(5);
        assertEquals(15, metrics.bytesRead());
    }

    @Test
    public void test_bytesWritten() {
        AsyncSocketMetrics metrics = new AsyncSocketMetrics();

        metrics.incBytesWritten(10);
        assertEquals(10, metrics.bytesWritten());

        metrics.incBytesWritten(5);
        assertEquals(15, metrics.bytesWritten());
    }

    @Test
    public void test_writeEvents() {
        AsyncSocketMetrics metrics = new AsyncSocketMetrics();

        metrics.incWriteEvents();
        assertEquals(1, metrics.writeEvents());

        metrics.incWriteEvents();
        assertEquals(2, metrics.writeEvents());
    }

    @Test
    public void test_readEvents() {
        AsyncSocketMetrics metrics = new AsyncSocketMetrics();

        metrics.incReadEvents();
        assertEquals(1, metrics.readEvents());

        metrics.incReadEvents();
        assertEquals(2, metrics.readEvents());
    }
}
