/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.starter.constructor.test;

import com.hazelcast.config.Config;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.starter.constructor.HazelcastPropertiesConstructor;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastPropertiesConstructorTest {

    @Test
    public void testConstructor() {
        Config config = new Config()
                .setProperty("myProperty", "myValue");
        HazelcastProperties properties = new HazelcastProperties(config);

        HazelcastPropertiesConstructor constructor = new HazelcastPropertiesConstructor(HazelcastProperties.class);
        HazelcastProperties clonedProperties = (HazelcastProperties) constructor.createNew(properties);

        assertEquals(properties.get("myProperty"), clonedProperties.get("myProperty"));
        assertEquals(properties.get("invalid"), clonedProperties.get("invalid"));
    }
}
