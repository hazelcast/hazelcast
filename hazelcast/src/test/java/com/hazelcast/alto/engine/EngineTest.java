package com.hazelcast.alto.engine;

import com.hazelcast.internal.tpc.TpcEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.sleepMillis;
import static com.hazelcast.internal.tpc.TpcEngine.State.SHUTDOWN;
import static com.hazelcast.internal.tpc.TpcEngine.State.TERMINATED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class EngineTest {

    private TpcEngine engine;

    @After
    public void after() {
        if (engine != null) {
            engine.shutdown();
        }
    }

    // ===================== start =======================

    @Test
    public void start_whenNew() {
        engine = new TpcEngine();
        engine.start();
        assertEquals(TpcEngine.State.RUNNING, engine.state());
    }

    @Test(expected = IllegalStateException.class)
    public void start_whenRunning() {
        engine = new TpcEngine();
        engine.start();
        engine.start();
    }

    // ================= shut down =======================

    @Test
    public void shutdown_whenNew() {
        engine = new TpcEngine();
        engine.shutdown();
        assertEquals(TERMINATED, engine.state());
    }

    @Test
    public void shutdown_whenRunning() throws InterruptedException {
        engine = new TpcEngine();
        engine.start();
        engine.eventloop(0).offer(() -> {
            sleepMillis(1000);
        });
        engine.shutdown();
        assertEquals(SHUTDOWN, engine.state());
        assertTrue(engine.awaitTermination(5, TimeUnit.SECONDS));
        assertEquals(TERMINATED, engine.state());
    }

    @Test
    public void shutdown_whenShutdown() throws InterruptedException {
        engine = new TpcEngine();
        engine.start();
        engine.eventloop(0).offer(() -> {
            sleepMillis(1000);
        });
        engine.shutdown();

        engine.shutdown();
        assertEquals(SHUTDOWN, engine.state());
        assertTrue(engine.awaitTermination(5, TimeUnit.SECONDS));
        assertEquals(TERMINATED, engine.state());
    }

    @Test
    public void shutdown_whenTerminated() {
        engine = new TpcEngine();
        engine.shutdown();

        engine.shutdown();
        assertEquals(TERMINATED, engine.state());
    }
}
