/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SqlResultImplTest extends SqlTestSupport {

    @Test
    public void test_updateCountResult() {
        SqlResultImpl r = SqlResultImpl.createUpdateCountResult(10);
        assertEquals(10, r.updateCount());

        assertIllegalStateException("This result contains only update count", r::getQueryId);
        assertIllegalStateException("This result contains only update count", r::getRowMetadata);
        assertIllegalStateException("This result contains only update count", r::iterator);
        r.close();
    }

    private void assertIllegalStateException(String expectedMessage, Runnable action) {
        IllegalStateException err = assertThrows(IllegalStateException.class, action);
        assertEquals(expectedMessage, err.getMessage());
    }
}
