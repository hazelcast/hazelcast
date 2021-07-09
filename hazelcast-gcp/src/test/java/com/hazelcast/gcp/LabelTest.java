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

package com.hazelcast.gcp;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LabelTest {
    @Test
    public void label() {
        // given
        String label = "key=value";

        // when
        Label result = new Label(label);

        // then
        assertEquals("key", result.getKey());
        assertEquals("value", result.getValue());
    }

    @Test
    public void labelWithWhitespaces() {
        // given
        String label = "   key = value ";

        // when
        Label result = new Label(label);

        // then
        assertEquals("key", result.getKey());
        assertEquals("value", result.getValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidLabel() {
        // given
        String label = "key";

        // when
        new Label(label);

        // then
        // throw exception
    }
}