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

package com.hazelcast.internal.tpcengine.iouring;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class UringTest {

    private Uring uring;

    @After
    public void after() {
        if (uring != null) {
            uring.close();
        }
    }

    @Test
    public void test_constructor_whenEntriesNotPowerOf2() {
        assertThrows(IllegalArgumentException.class, () -> new Uring(5, 0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_constructor_withZeroEntries() {
        new Uring(0, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_constructor_withNegativeEntries() {
        new Uring(-1, 0);
    }


    @Test(expected = IllegalArgumentException.class)
    public void test_constructor_withNegativeFlags() {
        new Uring(16, -1);
    }

    @Test
    public void test_constructor() {
        uring = new Uring(16384, 0);

        assertTrue("uring.ring_fd should be larger than 0", uring.ringFd() > 0);
    }
}
