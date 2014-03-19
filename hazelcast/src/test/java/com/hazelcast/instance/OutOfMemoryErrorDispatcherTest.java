package com.hazelcast.instance;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OutOfMemoryHandler;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertArrayEquals;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class OutOfMemoryErrorDispatcherTest {

    @Before
    public void before() {
        OutOfMemoryErrorDispatcher.clear();
    }

    @Test
    public void onOutOfMemoryOutOfMemory(){
        OutOfMemoryHandler handler = mock(OutOfMemoryHandler.class);
        OutOfMemoryError oome = new OutOfMemoryError();
        HazelcastInstance hz1 = mock(HazelcastInstance.class);

        OutOfMemoryErrorDispatcher.register(hz1);
        OutOfMemoryErrorDispatcher.setHandler(handler);

        HazelcastInstance[] registeredInstances = OutOfMemoryErrorDispatcher.current();

        OutOfMemoryErrorDispatcher.onOutOfMemory(oome);

        //make sure the handler is called
        verify(handler).onOutOfMemory(oome, registeredInstances);
        //make sure that the registered instances are removed.
        assertArrayEquals(new HazelcastInstance[]{}, OutOfMemoryErrorDispatcher.current());
     }

    @Test
    public void register() {
        HazelcastInstance hz1 = mock(HazelcastInstance.class);
        HazelcastInstance hz2 = mock(HazelcastInstance.class);

        OutOfMemoryErrorDispatcher.register(hz1);
        assertArrayEquals(new HazelcastInstance[]{hz1}, OutOfMemoryErrorDispatcher.current());

        OutOfMemoryErrorDispatcher.register(hz2);
        assertArrayEquals(new HazelcastInstance[]{hz1, hz2}, OutOfMemoryErrorDispatcher.current());
    }


    @Test(expected = IllegalArgumentException.class)
    public void register_whenNull() {
        OutOfMemoryErrorDispatcher.register(null);
    }

    @Test
    public void deregister_Existing() {
        HazelcastInstance hz1 = mock(HazelcastInstance.class);
        HazelcastInstance hz2 = mock(HazelcastInstance.class);
        HazelcastInstance hz3 = mock(HazelcastInstance.class);
        OutOfMemoryErrorDispatcher.register(hz1);
        OutOfMemoryErrorDispatcher.register(hz2);
        OutOfMemoryErrorDispatcher.register(hz3);

        OutOfMemoryErrorDispatcher.deregister(hz2);
        assertArrayEquals(new HazelcastInstance[]{hz1, hz3}, OutOfMemoryErrorDispatcher.current());

        OutOfMemoryErrorDispatcher.deregister(hz1);
        assertArrayEquals(new HazelcastInstance[]{hz3}, OutOfMemoryErrorDispatcher.current());

        OutOfMemoryErrorDispatcher.deregister(hz3);
        assertArrayEquals(new HazelcastInstance[]{}, OutOfMemoryErrorDispatcher.current());
    }

    @Test
    public void deregister_nonExisting() {
        HazelcastInstance instance = mock(HazelcastInstance.class);
        OutOfMemoryErrorDispatcher.deregister(instance);
    }

    @Test(expected = IllegalArgumentException.class)
    public void deregister_null() {
        OutOfMemoryErrorDispatcher.deregister(null);
    }
}
