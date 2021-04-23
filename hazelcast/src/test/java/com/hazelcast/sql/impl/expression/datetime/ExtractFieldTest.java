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

package com.hazelcast.sql.impl.expression.datetime;

import org.junit.Test;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.Assert.assertEquals;

public class ExtractFieldTest {

    @Test
    public void test_century() {
        assertEquals(-1, ExtractField.CENTURY.extract(OffsetDateTime.of(-1, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)), 0);
        assertEquals(-1, ExtractField.CENTURY.extract(OffsetDateTime.of(0, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)), 0);
        assertEquals(20, ExtractField.CENTURY.extract(OffsetDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)), 0);
        assertEquals(20, ExtractField.CENTURY.extract(OffsetDateTime.of(2000, 12, 31, 0, 0, 0, 0, ZoneOffset.UTC)), 0);
        assertEquals(21, ExtractField.CENTURY.extract(OffsetDateTime.of(2001, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)), 0);
    }
}
