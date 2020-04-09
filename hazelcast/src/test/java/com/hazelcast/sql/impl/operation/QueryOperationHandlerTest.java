/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.operation;

import com.hazelcast.sql.impl.SqlTestSupport;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * A set of integration tests for query message processing on a single member.
 * <p>
 * Abbreviations:
 * <ul>
 *     <li>E - execute</li>
 *     <li>Bx - batch request with x ordinal</li>
 *     <li>C - cancel</li>
 * </ul>
 */
public class QueryOperationHandlerTest extends SqlTestSupport {
    @Test
    public void test_E_B1_B2_C() {
        // TODO
    }

    @Test
    public void test_E_B1_C_B2() {
        // TODO
    }

    @Test
    public void test_E_C_B1_B2() {
        // TODO
    }

    @Ignore("https://github.com/hazelcast/hazelcast/issues/16868")
    @Test
    public void test_C_E_B1_B2() {
        fail("Cannot handle reordered cancel -> execute");
    }

    @Test
    public void test_B1_E_B2_C() {
        // TODO
    }

    @Test
    public void test_B1_E_C_B2() {
        // TODO
    }

    @Ignore("https://github.com/hazelcast/hazelcast/issues/16868")
    @Test
    public void test_B1_C_E_B2() {
        fail("Cannot handle reordered cancel -> execute");
    }

    @Ignore("https://github.com/hazelcast/hazelcast/issues/16868")
    @Test
    public void test_C_B1_E_B2() {
        fail("Cannot handle reordered cancel -> execute");
    }

    @Test
    public void test_B1_B2_E_C() {
        // TODO
    }

    @Ignore("https://github.com/hazelcast/hazelcast/issues/16868")
    @Test
    public void test_B1_B2_C_E() {
        fail("Cannot handle reordered cancel -> execute");
    }

    @Ignore("https://github.com/hazelcast/hazelcast/issues/16868")
    @Test
    public void test_B1_C_B2_E() {
        fail("Cannot handle reordered cancel -> execute");
    }

    @Ignore("https://github.com/hazelcast/hazelcast/issues/16868")
    @Test
    public void test_C_B1_B2_E() {
        fail("Cannot handle reordered cancel -> execute");
    }
}
