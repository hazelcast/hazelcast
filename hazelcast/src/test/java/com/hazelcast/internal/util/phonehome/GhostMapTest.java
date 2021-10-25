package com.hazelcast.internal.util.phonehome;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.Test;

import java.util.List;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class GhostMapTest {

    /**
     * Verifies that GH issue https://github.com/hazelcast/hazelcast/issues/19833 is fixed, and PhoneHome does not create a map
     * called `asterisk#team*` if map `asterisk#team1` already exists (in this case the
     * node.config.mapConfigs.get("asterisk#team1") call returned a MapConfig that had the name `asterisk#team*`, and the name of
     * the map config was in turn used later to fetch a map (for obtaining its LocalMapStats).
     */
    @Test
    public void test_GH_19833() {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(new Config()
                .addMapConfig(new MapConfig()
                        .setName("asterisk#team*")
                        .setTimeToLiveSeconds(10)
                )
        );
        instance1.getMap("asterisk#team1");

        new PhoneHome(TestUtil.getNode(instance1)).phoneHome(true);

        List<String> actualObjNames = instance1.getDistributedObjects().stream().map(
                DistributedObject::getName).collect(toList());
        assertEquals(singletonList("asterisk#team1"), actualObjNames);
    }
}
