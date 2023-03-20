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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OSTest {

    @Test
    public void test_isLinux0() {
        assertTrue(OS.isLinux0("Linux"));
        assertTrue(OS.isLinux0("LINUX"));
        assertTrue(OS.isLinux0("LiNuX"));
        assertTrue(OS.isLinux0("linux"));
        assertFalse(OS.isLinux0("Windows 10"));
        assertFalse(OS.isLinux0("Mac OS X"));
    }

    @Test
    public void test_isWindows0() {
        assertTrue(OS.isWindows0("Windows"));
        assertTrue(OS.isWindows0("wInDoWs"));
        assertTrue(OS.isWindows0("Windows 10"));
        assertTrue(OS.isWindows0("Windows 11"));
        assertFalse(OS.isWindows0("LINUX"));
        assertFalse(OS.isWindows0("LiNuX"));
        assertFalse(OS.isWindows0("linux"));
        assertFalse(OS.isWindows0("Mac OS X"));
    }

    @Test
    public void test_isMac() {
        assertFalse(OS.isMac0("Windows"));
        assertFalse(OS.isMac0("wInDoWs"));
        assertFalse(OS.isMac0("Windows 10"));
        assertFalse(OS.isMac0("Windows 11"));
        assertFalse(OS.isMac0("LINUX"));
        assertFalse(OS.isMac0("LiNuX"));
        assertFalse(OS.isMac0("linux"));
        assertTrue(OS.isMac0("Mac OS X"));
    }

    @Test
    public void test_linuxMajorVersion0_whenIsLinux() {
        assertEquals(5, OS.linuxMajorVersion0("5.16.12-200.fc35.x86_64", true));
    }

    @Test
    public void test_linuxMajorVersion0_whenNotIsLinux() {
        assertEquals(-1, OS.linuxMajorVersion0("5.16.12-200.fc35.x86_64", false));
    }

    @Test
    public void test_linuxMinorVersion0_whenisLinux() {
        assertEquals(16, OS.linuxMinorVersion0("5.16.12-200.fc35.x86_64", true));
    }

    @Test
    public void test_linuxMinorVersion0_whenNotIsLinux() {
        assertEquals(-1, OS.linuxMinorVersion0("5.16.12-200.fc35.x86_64", false));
    }

    @Test
    public void test_pageSize() {
        assertEquals(UnsafeLocator.UNSAFE.pageSize(), OS.pageSize());
    }
}
