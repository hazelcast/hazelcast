package com.hazelcast.internal.management;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.LongSupplier;

import static com.hazelcast.internal.management.ManagementCenterServiceIntegrationTest.MC_1_REMOTE_ADDR;
import static com.hazelcast.internal.management.ManagementCenterServiceIntegrationTest.MC_2_REMOTE_ADDR;
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
        eventStore = new ManagementCenterService.MCEventStore(clock, new LinkedBlockingQueue<>());
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
    }
}
