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

public class PortRangeTest {
    @Test
    public void portNumber() {
        // given
        int portNumber = 12345;
        String spec = String.valueOf(portNumber);

        // when
        PortRange portRange = new PortRange(spec);

        // then
        assertEquals(portNumber, portRange.getFromPort());
        assertEquals(portNumber, portRange.getToPort());
    }

    @Test(expected = IllegalArgumentException.class)
    public void portNumberOutOfPortRange() {
        new PortRange("12345678");
    }

    @Test(expected = IllegalArgumentException.class)
    public void portNumberOutOfIntegerRange() {
        new PortRange("123456789012356789123456789");
    }

    @Test
    public void portRange() {
        // given
        String spec = "123-456";

        // when
        PortRange portRange = new PortRange(spec);

        // then
        assertEquals(123, portRange.getFromPort());
        assertEquals(456, portRange.getToPort());
    }

    @Test(expected = IllegalArgumentException.class)
    public void portRangeFromPortOutOfRange() {
        new PortRange("12345678-1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void portRangeToPortOutOfRange() {
        new PortRange("1-123456789");
    }

    @Test(expected = IllegalArgumentException.class)
    public void portRangeFromPortGreaterThanToPort() {
        new PortRange("2-1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidSpec() {
        new PortRange("abcd");
    }
}