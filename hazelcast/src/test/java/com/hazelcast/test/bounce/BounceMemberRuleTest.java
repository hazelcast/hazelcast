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

package com.hazelcast.test.bounce;

import com.hazelcast.config.Config;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.randomMapName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test basic BounceMemberRule functionality
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BounceMemberRuleTest {

    private String mapName = randomMapName();

    @Rule
    public BounceMemberRule bounceMemberRule = BounceMemberRule.with(new Config()).clusterSize(3)
            .driverType(BounceTestConfiguration.DriverType.MEMBER).build();

    @Before
    public void setup() {
        IMap<String, String> stringMap = getMapFromSteadyMember();
        stringMap.put("1", "1");
    }

    @After
    public void tearDown() {
        IMap<String, String> stringMap = getMapFromSteadyMember();
        assertEquals(1, stringMap.size());
        stringMap.clear();
        assertEquals(0, stringMap.size());
    }

    @Test
    public void test_mapContainsExpectedKey() {
        assertTrue(getMapFromTestDriver().containsKey("1"));
    }

    @Test(expected = AssertionError.class)
    public void fails_immediately() {
        assertFalse(getMapFromTestDriver().containsKey("1"));
    }

    @Test(expected = AssertionError.class)
    public void fails_fromRunnable() {
        bounceMemberRule.test(new Runnable[]{
                new Runnable() {
                    @Override
                    public void run() {
                        assertFalse(getMapFromTestDriver().containsKey("1"));
                    }
                },
        });
    }

    @Test(expected = AssertionError.class)
    public void test_cannotSubmit_afterTasksAlreadySubmitted() {
        Runnable task = new Runnable() {
            @Override
            public void run() {
                // do nothing
            }
        };
        bounceMemberRule.test(new Runnable[]{
                task,
        });
        // next statement will throw an AssertionError
        bounceMemberRule.test(new Runnable[]{
                task,
        });
    }

    @Test(expected = AssertionError.class)
    public void fails_whenRanRepeatedly() {
        bounceMemberRule.testRepeatedly(new Runnable[]{
                new Runnable() {
                    @Override
                    public void run() {
                        assertFalse(getMapFromTestDriver().containsKey("1"));
                    }
                },
        }, 10);
    }

    private IMap<String, String> getMapFromSteadyMember() {
        return bounceMemberRule.getSteadyMember().getMap(mapName);
    }

    private IMap<String, String> getMapFromTestDriver() {
        return bounceMemberRule.getNextTestDriver().getMap(mapName);
    }
}
