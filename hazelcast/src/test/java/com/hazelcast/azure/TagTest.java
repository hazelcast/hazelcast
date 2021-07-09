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

import com.hazelcast.config.InvalidConfigurationException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TagTest {
    @Test
    public void tag() {
        // given
        String tag = "key=value";

        // when
        Tag result = new Tag(tag);

        // then
        assertEquals("key", result.getKey());
        assertEquals("value", result.getValue());
    }

    @Test
    public void tagWithWhitespaces() {
        // given
        String tag = "   key = value ";

        // when
        Tag result = new Tag(tag);

        // then
        assertEquals("key", result.getKey());
        assertEquals("value", result.getValue());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void invalidTag() {
        // given
        String tag = "key";

        // when
        new Tag(tag);

        // then
        // throw exception
    }
}