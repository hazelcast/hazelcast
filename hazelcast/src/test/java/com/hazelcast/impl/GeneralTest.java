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

package com.hazelcast.impl;

import com.hazelcast.nio.Data;
import org.junit.Test;

import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GeneralTest {

    @Test
    public void testRequestHardCopy() {
        Data key = toData("key");
        Data value = toData("value");
        Request request = new Request();
        request.key = key;
        request.value = value;
        request.name = "somename";
        Request hardCopy = new Request();
        hardCopy.setFromRequest(request, true);
        assertEquals(request.key, hardCopy.key);
        assertEquals(request.value, hardCopy.value);
        assertTrue(hardCopy.key.size() > 0);
        assertTrue(hardCopy.value.size() > 0);
        assertEquals("value", toObject(hardCopy.value));
        hardCopy = new Request();
        hardCopy.setFromRequest(request, true);
        assertEquals(request.key, hardCopy.key);
        assertEquals(request.value, hardCopy.value);
        assertTrue(hardCopy.key.size() > 0);
        assertTrue(hardCopy.value.size() > 0);
        assertEquals("value", toObject(hardCopy.value));
    }
}
