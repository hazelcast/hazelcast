/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.core.IMap;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

public class MapBackupTest extends BaseTest {

    @Test
    public void testMapPutBackups() throws InterruptedException {
        IMap map = getInstance(0).getMap("testMapBackups");
        int size = 1000;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        Thread.sleep(1000);

        getInstance(1).getLifecycleService().shutdown();
        Thread.sleep(2000);
        for (int i = 0; i < size; i++) {
            assertEquals(i, map.get(i));
        }
        assertEquals(map.size(), size);
    }

}
