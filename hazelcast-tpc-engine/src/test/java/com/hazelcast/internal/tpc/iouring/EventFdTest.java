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

package com.hazelcast.internal.tpc.iouring;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class EventFdTest {
    private EventFd eventFd;

    @After
    public void after() {
        if (eventFd != null) {
            eventFd.close();
        }
    }

    @Test
    public void test_constructor() {
        eventFd = new EventFd();
        assertTrue(eventFd.fd() >= 0);
    }

    @Test
    public void test_close_whenAlreadyClosed() {
        eventFd = new EventFd();
        eventFd.close();
        eventFd.close();
    }
}
