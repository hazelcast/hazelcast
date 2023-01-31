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
