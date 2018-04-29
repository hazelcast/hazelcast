/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ConfigTest extends HazelcastTestSupport {

    /**
     * Tests that the order of configuration creation matters.
     * <ul>
     * <li>Configurations which are created before the "default" configuration do not inherit from it.</li>
     * <li>Configurations which are created after the "default" configuration do inherit from it.</li>
     * </ul>
     */
    @Test
    public void testInheritanceFromDefaultConfig() {
        assertNotEquals("Expected that the default in-memory format is not OBJECT",
                MapConfig.DEFAULT_IN_MEMORY_FORMAT, InMemoryFormat.OBJECT);

        Config config = new Config();
        config.getMapConfig("myBinaryMap")
                .setBackupCount(3);
        config.getMapConfig("default")
                .setInMemoryFormat(InMemoryFormat.OBJECT);
        config.getMapConfig("myObjectMap")
                .setBackupCount(5);

        HazelcastInstance hz = createHazelcastInstance(config);

        MapConfig binaryMapConfig = hz.getConfig().findMapConfig("myBinaryMap");
        assertEqualsStringFormat("Expected %d sync backups, but found %d", 3, binaryMapConfig.getBackupCount());
        assertEqualsStringFormat("Expected %s in-memory format, but found %s",
                MapConfig.DEFAULT_IN_MEMORY_FORMAT, binaryMapConfig.getInMemoryFormat());

        MapConfig objectMapConfig = hz.getConfig().findMapConfig("myObjectMap");
        assertEqualsStringFormat("Expected %d sync backups, but found %d", 5, objectMapConfig.getBackupCount());
        assertEqualsStringFormat("Expected %s in-memory format, but found %s",
                InMemoryFormat.OBJECT, objectMapConfig.getInMemoryFormat());
    }
}
