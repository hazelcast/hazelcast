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

package com.hazelcast.aggregation;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.test.bounce.BounceTestConfiguration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.aggregation.AggregatorsSpecTest.PERSONS_COUNT;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class AggregationMemberBounceTest extends HazelcastTestSupport {
    private static final int MEMBER_COUNT = 4;
    private static final int DRIVER_COUNT = 4;
    private static final int TEST_DURATION_SECONDS = 40;

    private String mapName;

    @Rule
    public BounceMemberRule bounceMemberRule = BounceMemberRule.with(getConfig())
            .clusterSize(MEMBER_COUNT)
            .driverCount(DRIVER_COUNT).driverType(BounceTestConfiguration.DriverType.MEMBER).build();

    protected Config getConfig() {
        return new Config();
    }

    @Before
    public void setup() {
        mapName = randomMapName();
        final HazelcastInstance steadyMember = bounceMemberRule.getSteadyMember();
        IMap<Integer, AggregatorsSpecTest.Person> map = steadyMember.getMap(mapName);
        AggregatorsSpecTest.populateMapWithPersons(map, "", PERSONS_COUNT);
    }

    @Test
    public void aggregationReturnsCorrectResultWhenBouncing() {

        Runnable[] runnables = new Runnable[DRIVER_COUNT];
        for (int i = 0; i < DRIVER_COUNT; i++) {
            HazelcastInstance driver = bounceMemberRule.getNextTestDriver();
            final IMap<Integer, AggregatorsSpecTest.Person> map = driver.getMap(mapName);
            runnables[i] = new Runnable() {
                @Override
                public void run() {
                    String postfix = "";
                    AggregatorsSpecTest.assertMinAggregators(map, postfix, Predicates.alwaysTrue());
                    AggregatorsSpecTest.assertMaxAggregators(map, postfix, Predicates.alwaysTrue());
                    AggregatorsSpecTest.assertSumAggregators(map, postfix, Predicates.alwaysTrue());
                    AggregatorsSpecTest.assertAverageAggregators(map, postfix, Predicates.alwaysTrue());
                    AggregatorsSpecTest.assertCountAggregators(map, postfix, Predicates.alwaysTrue());
                    AggregatorsSpecTest.assertDistinctAggregators(map, postfix, Predicates.alwaysTrue());
                }
            };
        }
        bounceMemberRule.testRepeatedly(runnables, TEST_DURATION_SECONDS);
    }
}
