package com.hazelcast.map.impl.query;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RootCauseMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.lang.reflect.Constructor;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class TargetTest {

    @Rule
    public ExpectedException rule = ExpectedException.none();

    @Test
    public void testConstructor_withInvalidPartitionId() throws Exception {
        // retrieve the wanted constructor and make it accessible
        Constructor<Target> constructor = Target.class.getDeclaredConstructor(Target.TargetFlag.class, Integer.class);
        constructor.setAccessible(true);

        // we expect an IllegalArgumentException to be thrown
        rule.expect(new RootCauseMatcher(IllegalArgumentException.class));
        constructor.newInstance(Target.TargetFlag.PARTITION_OWNER, null);
    }
}
