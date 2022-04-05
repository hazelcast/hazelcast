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

package com.hazelcast.spring.cache;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.spring.CustomSpringJUnit4ClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

/**
 * Tests for {@link HazelcastCache} for timeout.
 *
 * @author Gokhan Oner
 */
@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"readtimeout-config.xml"})
@Category(QuickTest.class)
public class HazelcastCacheReadTimeoutTest extends AbstractHazelcastCacheReadTimeoutTest {

    @BeforeClass
    public static void start() {
        System.setProperty(HazelcastCacheManager.CACHE_PROP, "defaultReadTimeout=100,delay150=150,delay50=50,delayNo=0");
        Hazelcast.shutdownAll();
    }


    @AfterClass
    public static void end() {
        System.clearProperty(HazelcastCacheManager.CACHE_PROP);
        Hazelcast.shutdownAll();
    }


}
