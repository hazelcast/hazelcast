/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MapMaxSizeConfigTest extends HazelcastTestSupport {

    @Test
    public void setMaxSize_withConstructor_toZero() throws Exception {
        MaxSizeConfig maxSizeConfig = new MaxSizeConfig(0, MaxSizeConfig.MaxSizePolicy.PER_NODE);

        assertEquals(Integer.MAX_VALUE, maxSizeConfig.getSize());
    }

    @Test
    public void setMaxSize_withSetter_toZero() throws Exception {
        MaxSizeConfig maxSizeConfig = new MaxSizeConfig();
        maxSizeConfig.setSize(0);

        assertEquals(Integer.MAX_VALUE, maxSizeConfig.getSize());
    }

    @Test
    public void setMaxSize_withConstructor_toNegative() throws Exception {
        MaxSizeConfig maxSizeConfig = new MaxSizeConfig(-2131, MaxSizeConfig.MaxSizePolicy.PER_NODE);

        assertEquals(Integer.MAX_VALUE, maxSizeConfig.getSize());
    }

    @Test
    public void setMaxSize_withSetter_toNegative() throws Exception {
        MaxSizeConfig maxSizeConfig = new MaxSizeConfig();
        maxSizeConfig.setSize(-2131);

        assertEquals(Integer.MAX_VALUE, maxSizeConfig.getSize());
    }

    @Test
    public void setMaxSize_withConstructor_toPositive() throws Exception {
        final int expectedMaxSize = 123456;
        MaxSizeConfig maxSizeConfig = new MaxSizeConfig(expectedMaxSize, MaxSizeConfig.MaxSizePolicy.PER_NODE);

        assertEquals(expectedMaxSize, maxSizeConfig.getSize());
    }

    @Test
    public void setMaxSize_withSetter_toPositive() throws Exception {
        final int expectedMaxSize = 123456;
        MaxSizeConfig maxSizeConfig = new MaxSizeConfig();
        maxSizeConfig.setSize(expectedMaxSize);

        assertEquals(expectedMaxSize, maxSizeConfig.getSize());
    }
}
