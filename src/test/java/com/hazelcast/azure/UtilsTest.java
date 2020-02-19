/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.hazelcast.azure;

import org.junit.Test;

import static com.hazelcast.azure.Utils.isAllBlank;
import static com.hazelcast.azure.Utils.isAllNotBlank;
import static com.hazelcast.azure.Utils.isBlank;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UtilsTest {
    @Test
    public void isBlankTest(){
        assertFalse(isBlank("test-string"));
        assertFalse(isBlank(" test-string-with-initial-whitespace"));
        assertTrue(isBlank(""));
        assertTrue(isBlank(null));
    }

    @Test
    public void isAllBlankTest(){
        assertFalse(isAllBlank("test-string-1", "test-string-2"));
        assertFalse(isAllBlank("test-string-1", ""));
        assertTrue(isAllBlank("", "", null));
    }

    @Test
    public void isAllNotBlankTest(){
        assertTrue(isAllNotBlank("test-string-1", "test-string-2"));
        assertFalse(isAllNotBlank("test-string-1", ""));
        assertFalse(isAllNotBlank("", "", null));
    }


}