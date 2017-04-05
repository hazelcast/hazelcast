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

package com.hazelcast.jet.stream;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class ForEachTest extends AbstractStreamTest {

    @Test
    public void sourceMap() {
        final AtomicInteger runningTotal = new AtomicInteger(0);
        streamMap().forEach(e -> runningTotal.addAndGet(e.getValue()));

        assertEquals((COUNT - 1) * (COUNT) / 2, runningTotal.get());
    }

    @Test
    public void sourceCache() {
        final AtomicInteger runningTotal = new AtomicInteger(0);
        streamCache().forEach(e -> runningTotal.addAndGet(e.getValue()));

        assertEquals((COUNT - 1) * (COUNT) / 2, runningTotal.get());
    }

    @Test
    public void sourceList() {
        final AtomicInteger runningTotal = new AtomicInteger(0);
        streamList().forEach(runningTotal::addAndGet);

        assertEquals((COUNT - 1) * (COUNT) / 2, runningTotal.get());
    }

}
