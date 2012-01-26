/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.util;

import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for AddressUtil class.
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class AddressUtilTest {

    @Test
    public void testSimple() throws Exception {
        final List<String> members = AddressUtil.handleMember("10.11.12.13");
        assertEquals(1, members.size());
        assertEquals("10.11.12.13", members.get(0));
    }

    @Test
    public void testEvery() throws Exception {
        final List<String> members = AddressUtil.handleMember("10.11.12.*");
        assertEquals(256, members.size());
        int i = 0;
        for (final String member : members) {
            assertEquals("10.11.12." + (i++), member);
        }
    }

    @Test
    public void testRange() throws Exception {
        final List<String> members = AddressUtil.handleMember("10.11.12.15-20");
        assertEquals(6, members.size());
        int i = 15;
        for (final String member : members) {
            assertEquals("10.11.12." + (i++), member);
        }
    }
}
