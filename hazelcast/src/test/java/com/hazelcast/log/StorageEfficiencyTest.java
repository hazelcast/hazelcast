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
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StorageEfficiencyTest extends HazelcastTestSupport {

    @Test
    public void whenMany() {
        LogConfig objectLogConfig = new LogConfig("object")
                .setSegmentSize(1024);
        LogConfig longLogConfig = new LogConfig("long")
                .setType(Long.TYPE)
                .setSegmentSize(1024);

        Config config = new Config().addLogConfig(objectLogConfig).addLogConfig(longLogConfig);
        HazelcastInstance hz = createHazelcastInstance(config);

        Log<Long> objectLog = hz.getLog(objectLogConfig.getName());
        Log<Long> longLog = hz.getLog(longLogConfig.getName());
        long count = 100000;
        for (long k = 0; k < count; k++) {
            objectLog.put(0, k);
            longLog.put(0, k);
        }

        assertEquals(8 * count, longLog.usage(0).bytesInUse);
        assertEquals(20 * count, objectLog.usage(0).bytesInUse);
    }
}
