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

import org.junit.Test;

import java.io.File;
import java.util.UUID;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class LinuxTest {

    @Test
    public void test_eventfd() {
        int fd = Linux.eventfd(0, 0);
        assertNotEquals(-1, fd);
        Linux.close(fd);
    }

    @Test
    public void test_open() {
        File tmpdir = new File(System.getProperty("java.io.tmpdir"));
        String filename = tmpdir + "/" + UUID.randomUUID() + ".tmp";
        int fd = Linux.open(filename, Linux.O_CREAT, Linux.PERMISSIONS_ALL);
        assertNotEquals(-1, fd);

        File file = new File(filename);
        assertTrue(file.exists());

        file.delete();
    }
}
