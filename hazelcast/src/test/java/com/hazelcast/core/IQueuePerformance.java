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

package com.hazelcast.core;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class IQueuePerformance extends PerformanceTest {
    private IQueue<String> queue = Hazelcast.getQueue("IQueuePerformance");

    @After
    public void clear() {
        queue.clear();
        t.stop();
        t.printResult();
    }

    @Test
    public void testQueueAdd() {
        t = new PerformanceTimer("testQueueAdd", ops);
        for (int i = 0; i < ops; ++i) {
            queue.add("Hello");
        }
    }
}
