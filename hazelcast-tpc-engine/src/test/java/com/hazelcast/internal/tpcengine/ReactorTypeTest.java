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

package com.hazelcast.internal.tpcengine;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ReactorTypeTest {

    @Test
    public void test_fromString() {
        assertEquals(ReactorType.NIO, ReactorType.fromString("nio"));
        assertEquals(ReactorType.NIO, ReactorType.fromString("NIO"));
        assertEquals(ReactorType.IOURING, ReactorType.fromString("iouring"));
        assertEquals(ReactorType.IOURING, ReactorType.fromString("io_uring"));
        assertEquals(ReactorType.IOURING, ReactorType.fromString("IOURING"));
    }

    @Test(expected = NullPointerException.class)
    public void test_fromString_whenNull() {
        ReactorType.fromString(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_fromString_whenBogusString() {
        ReactorType.fromString("bogus");
    }
}
