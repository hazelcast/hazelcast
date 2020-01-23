/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.log;

import com.hazelcast.config.Config;
import com.hazelcast.config.LogConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.log.impl.UsageInfo;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ClearTest extends HazelcastTestSupport {

    @Test
    public void whenMany() {
        LogConfig logConfig = new LogConfig("foo")
                .setSegmentSize(1024);
        HazelcastInstance hz = createHazelcastInstance(new Config().addLogConfig(logConfig));

        Log<String> log = hz.getLog("foo");
        long count = 100000;
        for (int k = 0; k < count; k++) {
            log.put("" + k);
        }

        log.clear();

        assertEquals(0, log.count());
        UsageInfo info = log.usage();
        assertEquals(0, info.count);
        assertEquals(0, info.segments);
        assertEquals(0, info.bytesInUse);
    }
}
