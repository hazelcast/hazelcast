package com.hazelcast.map.impl.client;

import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapPortableHookTest {

    @Test(expected = IndexOutOfBoundsException.class)
    public void testPortableFactory_whenCreatingUnregisteredConstructor_thenThrowException() {
        MapPortableHook hook = new MapPortableHook();
        PortableFactory factory = hook.createFactory();

        factory.create(-1);
    }
}
