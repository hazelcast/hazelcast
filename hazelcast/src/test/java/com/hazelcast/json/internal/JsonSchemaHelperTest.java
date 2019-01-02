/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.json.internal;

import com.hazelcast.internal.json.PrettyPrint;
import com.hazelcast.internal.json.RandomPrint;
import com.hazelcast.internal.json.WriterConfig;
import com.hazelcast.nio.BufferObjectDataInput;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * This test automatically tests {@link JsonSchemaHelper#findPattern(BufferObjectDataInput, int, JsonSchemaDescription, String[])}
 * and {@link JsonSchemaHelper#findValueWithPattern(BufferObjectDataInput, int, JsonSchemaDescription, List, String[])}
 * methods.
 *
 * It runs the mentioned methods on pre-determined {@code JsonValue}s.
 * The tests use all valid attribute paths to extract {@code JsonValue}s.
 */
public class JsonSchemaHelperTest extends AbstractJsonSchemaTest {

    @Test
    public void testOne() throws IOException {
        testOne(12);
    }

    @Test
    public void testAllValidPaths_MinimalPrint() throws IOException {
        testPaths(WriterConfig.MINIMAL);
    }

    @Test
    public void testAllValidPaths_PrettyPrint() throws IOException {
        testPaths(PrettyPrint.PRETTY_PRINT);
    }

    @Test
    public void testAllValidPaths_RandomPrint() throws IOException {
        testPaths(RandomPrint.RANDOM_PRINT);
    }
}
