/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.aws;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TagTest {
    @Test
    public void tagKeyOnly() {
        // given
        String key = "Key";
        String value = null;

        // when
        Tag tag = new Tag(key, value);

        // then
        assertEquals(tag.getKey(), key);
        assertNull(tag.getValue());
    }

    @Test
    public void tagValueOnly() {
        // given
        String key = null;
        String value = "Value";

        // when
        Tag tag = new Tag(key, value);

        // then
        assertNull(tag.getKey());
        assertEquals(tag.getValue(), value);
    }

    @Test(expected = IllegalArgumentException.class)
    public void missingKeyAndValue() {
        // given
        String key = null;
        String value = null;

        // when
        new Tag(key, value);

        // then
        // throws exception
    }
}
