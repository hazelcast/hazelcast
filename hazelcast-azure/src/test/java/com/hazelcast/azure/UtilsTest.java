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

import static com.hazelcast.azure.Utils.isAllFilled;
import static com.hazelcast.azure.Utils.isAnyFilled;
import static com.hazelcast.azure.Utils.isEmpty;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UtilsTest {
    @Test
    public void isEmptyTest(){
        assertFalse(isEmpty("test-string"));
        assertFalse(isEmpty(" test-string-with-initial-whitespace"));
        assertTrue(isEmpty(""));
        assertTrue(isEmpty(null));
    }

    @Test
    public void isAllFilledTest(){
        assertTrue(isAllFilled("test-string-1", "test-string-2"));
        assertFalse(isAllFilled("test-string-1", ""));
        assertFalse(isAllFilled("", "", null));
    }

    @Test
    public void isAnyFilledTest(){
        assertTrue(isAnyFilled("test-string-1", "test-string-2"));
        assertTrue(isAnyFilled("test-string-1", ""));
        assertFalse(isAnyFilled("", "", null));
    }

}