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

package com.hazelcast.internal.util;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class Sha256UtilTest {
    @Test
    public void testBytesToHex() {
        byte[] data = {3, -61, -37, -66, 125, -120, 21, -109, 126, 53, 75, -115, 44, 76, -17, -53, 2, 6, 61, -45, 32,
                -19, 35, -15, 109, -114, 92, -13, 109, -44, -7, 42};
        String result = Sha256Util.bytesToHex(data);
        assertEquals("The result must has even length", 0, result.length() % 2);
        assertEquals("03c3dbbe7d8815937e354b8d2c4cefcb02063dd320ed23f16d8e5cf36dd4f92a", result);
    }

    @Test
    public void testCalculateSha256Hex() throws Exception {
        byte[] data = {(byte) 0};
        String result = Sha256Util.calculateSha256Hex(data);
        assertEquals("6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d", result);
    }

    @Test
    public void testLeadingZeroWithLength() throws Exception {
        byte[] data = {11, 52, -94, -104, 3, 89, -126, 7, 49, -84, 67, 111, -81, 15, 69, -19, 69, 99, -112, -110, -89,
                -42, 87, -12, 37, -114, -116, -47, -83, -28, 5, -83};
        String result = Sha256Util.calculateSha256Hex(data, 32);
        assertEquals("0dd0af6f7fe8a8816856fadf34cbf7ca5ff7c5af088da656c94c49ff60aea20f", result);
    }

    @Test
    public void testLeadingZero() throws Exception {
        byte[] data = {-103, -109, 6, 90, -72, 68, 41, 7, -45, 42, 12, -38, -50, 123, -100, 102, 95, 65, 5, 30, 64, 85,
                126, -26, 5, 54, 18, -98, -85, -101, 109, -91};
        String result = Sha256Util.calculateSha256Hex(data);
        assertEquals("07b18fecd4bcb1a726fbab1bd4c017e57e20f6f962a342789c57e531667f603b", result);
    }

    @Test
    public void testTwoLeadingZeros() throws Exception {
        byte[] data = {-67, 65, -32, -95, 16, 21, -123, 112, -40, -40, -58, -97, -59, 48, 100, 79, 67, -86, 68, 119,
                -104, 77, -63, 9, -55, -74, -27, 123, -125, 64, 85, -7};
        String result = Sha256Util.calculateSha256Hex(data);
        assertEquals("0078723ef3412533bfc5f362ce0de7d9e18b847a0360dfa4d9a37c3923585097", result);
    }

    @Test
    public void test_exception_whenDirectory() {
        Path path = Paths.get("src", "test", "resources");
        assertThrows(IOException.class, () -> Sha256Util.calculateSha256Hex(path));
    }
}
