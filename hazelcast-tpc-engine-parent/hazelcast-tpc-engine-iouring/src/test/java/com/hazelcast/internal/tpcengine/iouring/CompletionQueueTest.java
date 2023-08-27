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

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertSame;

public class CompletionQueueTest {

    @Test
    public void test() {
        int entries = 16384;
        Uring uring = new Uring(entries, 0);
        CompletionQueue cq = uring.completionQueue();

        assertSame(uring, cq.uring());
        Assert.assertEquals(2 * entries - 1, cq.ringMask());
    }
}
