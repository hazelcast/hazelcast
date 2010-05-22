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

package com.hazelcast.nio;

import org.junit.Test;

import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;
import static org.junit.Assert.assertEquals;

public class SerializationTest {
    @Test
    public void testLongValueIncrement() {
        Data zero = toData(0L);
        Data five = IOUtil.addDelta(zero, 5L);
        assertEquals (5L, toObject(five));
        Data minusThree = IOUtil.addDelta(five, -8L);
        assertEquals (-3L, toObject(minusThree));
        Data minusTwo = IOUtil.addDelta(minusThree, 1L);
        assertEquals (-2L, toObject(minusTwo));
        Data twenty = IOUtil.addDelta(minusThree, 23L);
        assertEquals (20L, toObject(twenty));
    }
}
