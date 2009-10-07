/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.nio;

import static com.hazelcast.nio.IOUtil.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;


public class IOTest {

    @Test
    public void testHardCopy() {
        Data data = toData("value");
        Data hardCopy = doHardCopy(data);

        assertEquals(data, hardCopy);
        assertEquals("value", toObject(hardCopy));
        assertEquals(data, hardCopy);
        assertEquals(data.size(), hardCopy.size());
        assertTrue(hardCopy.size() > 0);

        assertEquals("value", toObject(hardCopy));
        assertTrue(hardCopy.size() > 0);
    }
}
