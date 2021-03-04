package com.hazelcast.internal.management;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.management.events.Event;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.LongSupplier;

import static com.hazelcast.internal.management.ManagementCenterService.MCEventStore.MC_EVENTS_WINDOW_MILLIS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MCEventStoreTest {
    
    private static class FakeClock implements LongSupplier {
        
        long now = 42;

        @Override
        public long getAsLong() {
            return now;
        }
    }

    static final Address MC_1_REMOTE_ADDR;

    static final Address MC_2_REMOTE_ADDR;

    static final Address MC_3_REMOTE_ADDR;

    static {
        try {
            MC_1_REMOTE_ADDR = new Address("localhost", 5703);
            MC_2_REMOTE_ADDR = new Address("localhost", 5704);
            MC_3_REMOTE_ADDR = new Address("localhost", 5705);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
    
    private LinkedBlockingQueue<Event> queue;
    
    private ManagementCenterService.MCEventStore eventStore;
    
    private FakeClock clock;
    
    private ManagementCenterServiceIntegrationTest.TestEvent testEvent() {
        return new ManagementCenterServiceIntegrationTest.TestEvent(clock.now);
    }
    
    void inNextMilli(Runnable r) {
        clock.now++;
        r.run();
    }
    
    @Before
    public void before() {
        clock = new FakeClock();
        queue = new LinkedBlockingQueue<>();
        eventStore = new ManagementCenterService.MCEventStore(clock, queue);
    }

    @Test
    public void multipleMCs_canPollSeparately() {
        inNextMilli(() -> {
            eventStore.log(testEvent());
            eventStore.log(testEvent());    
        });
        inNextMilli(() -> {
            assertEquals(2, eventStore.pollMCEvents(MC_1_REMOTE_ADDR).size());
            assertEquals(2, eventStore.pollMCEvents(MC_2_REMOTE_ADDR).size());
            assertEquals(0, eventStore.pollMCEvents(MC_1_REMOTE_ADDR).size());
            assertEquals(0, eventStore.pollMCEvents(MC_2_REMOTE_ADDR).size());
        });
        inNextMilli(() -> {
            eventStore.log(testEvent());
            assertEquals(1, eventStore.pollMCEvents(MC_1_REMOTE_ADDR).size());                
        });
        inNextMilli(() -> {
            eventStore.log(testEvent());
            assertEquals(1, eventStore.pollMCEvents(MC_1_REMOTE_ADDR).size());
            assertEquals(2, eventStore.pollMCEvents(MC_2_REMOTE_ADDR).size());
        });
        inNextMilli(() -> {
            eventStore.log(testEvent());
            eventStore.log(testEvent());
        });
        clock.now += MC_EVENTS_WINDOW_MILLIS;
        eventStore.log(testEvent());
        inNextMilli(() -> {
            assertEquals(0, eventStore.pollMCEvents(MC_1_REMOTE_ADDR).size());
            assertEquals(0, eventStore.pollMCEvents(MC_2_REMOTE_ADDR).size());
        });
    }
    
    @Test
    public void elemsReadByAllMCsAreCleared() {
        inNextMilli(() -> {
            eventStore.log(testEvent());
            eventStore.log(testEvent());
        });
        
        inNextMilli(() -> {
            assertEquals(2, eventStore.pollMCEvents(MC_1_REMOTE_ADDR).size());
            assertEquals(2, eventStore.pollMCEvents(MC_2_REMOTE_ADDR).size());
            assertEquals(0, queue.size());
        });
    }
}
