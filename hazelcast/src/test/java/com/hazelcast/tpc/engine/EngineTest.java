package com.hazelcast.tpc.engine;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.sleepMillis;
import static com.hazelcast.tpc.engine.Engine.State.SHUTDOWN;
import static com.hazelcast.tpc.engine.Engine.State.TERMINATED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class EngineTest {

    private Engine engine;

    @After
    public void after() {
        if (engine != null) {
            engine.shutdown();
        }
    }

    // ===================== start =======================

    @Test
    public void start_whenNew() {
        engine = new Engine();
        engine.start();
        assertEquals(Engine.State.RUNNING, engine.state());
    }

    @Test(expected = IllegalStateException.class)
    public void start_whenRunning() {
        engine = new Engine();
        engine.start();
        engine.start();
    }

    // ================= shut down =======================

    @Test
    public void shutdown_whenNew() {
        engine = new Engine();
        engine.shutdown();
        assertEquals(TERMINATED, engine.state());
    }

    @Test
    public void shutdown_whenRunning() throws InterruptedException {
        engine = new Engine();
        engine.start();
        engine.eventloop(0).execute(() -> {
            sleepMillis(1000);
        });
        engine.shutdown();
        assertEquals(SHUTDOWN, engine.state());
        assertTrue(engine.awaitTermination(5, TimeUnit.SECONDS));
        assertEquals(TERMINATED, engine.state());
    }

    @Test
    public void shutdown_whenShutdown() throws InterruptedException {
        engine = new Engine();
        engine.start();
        engine.eventloop(0).execute(() -> {
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
        engine = new Engine();
        engine.shutdown();

        engine.shutdown();
        assertEquals(TERMINATED, engine.state());
    }
}
