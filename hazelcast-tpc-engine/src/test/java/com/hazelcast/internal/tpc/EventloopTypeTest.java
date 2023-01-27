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

package com.hazelcast.internal.tpc;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EventloopTypeTest {

    @Test
    public void test_fromString() {
        assertEquals(EventloopType.NIO, EventloopType.fromString("nio"));
        assertEquals(EventloopType.NIO, EventloopType.fromString("NIO"));
        assertEquals(EventloopType.IOURING, EventloopType.fromString("iouring"));
        assertEquals(EventloopType.IOURING, EventloopType.fromString("io_uring"));
        assertEquals(EventloopType.IOURING, EventloopType.fromString("IOURING"));
    }

    @Test(expected = NullPointerException.class)
    public void test_fromString_whenNull() {
        EventloopType.fromString(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_fromString_whenBogusString() {
        EventloopType.fromString("bogus");
    }
}
