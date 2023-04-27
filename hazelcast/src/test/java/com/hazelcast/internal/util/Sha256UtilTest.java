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
        String result = Sha256Util.calculateSha256Hex(data, data.length);
        assertEquals("6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d", result);
    }

    @Test
    public void test_exception_whenDirectory() {
        Path path = Paths.get("src", "test", "resources");
        assertThrows(IOException.class, () -> Sha256Util.calculateSha256Hex(path));
    }
}
