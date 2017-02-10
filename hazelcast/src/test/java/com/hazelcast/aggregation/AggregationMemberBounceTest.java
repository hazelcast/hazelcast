package com.hazelcast.aggregation;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

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
            .driverCount(DRIVER_COUNT).build();

    protected Config getConfig() {
        return new Config();
    }

    @Before
    public void setup() {
        mapName = randomMapName();
        final HazelcastInstance steadyMember = bounceMemberRule.getSteadyMember();
        IMap<Integer, AggregatorsSpecTest.Person> map = steadyMember.getMap(mapName);
        AggregatorsSpecTest.populateMapWithPersons(map);
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
                    AggregatorsSpecTest.assertMinAggregators(map);
                    AggregatorsSpecTest.assertMaxAggregators(map);
                    AggregatorsSpecTest.assertSumAggregators(map);
                    AggregatorsSpecTest.assertAverageAggregators(map);
                    AggregatorsSpecTest.assertCountAggregators(map);
                    AggregatorsSpecTest.assertDistinctAggregators(map);
                }
            };
        }
        bounceMemberRule.testRepeatedly(runnables, TEST_DURATION_SECONDS);
    }
}
