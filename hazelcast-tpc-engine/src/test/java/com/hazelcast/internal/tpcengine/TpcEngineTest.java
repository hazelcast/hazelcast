/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine;


import org.junit.After;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.tpcengine.TpcEngine.State.SHUTDOWN;
import static com.hazelcast.internal.tpcengine.TpcEngine.State.TERMINATED;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.assertTrueEventually;
import static com.hazelcast.internal.tpcengine.TpcTestSupport.sleepMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TpcEngineTest {

    private TpcEngine engine;

    @After
    public void after() throws InterruptedException {
        if (engine != null) {
            engine.shutdown();
            if (!engine.awaitTermination(10, TimeUnit.SECONDS)) {
                throw new RuntimeException("Failed to await termination due to timeout");
            }
        }
    }

    @Test
    public void test() {
        TpcEngineBuilder configuration = new TpcEngineBuilder();
        int reactorCount = 5;
        configuration.setReactorCount(reactorCount);

        engine = new TpcEngine(configuration);

        assertEquals(5, engine.reactors().length);
        assertEquals(reactorCount, engine.reactorCount());
        assertEquals(ReactorType.NIO, engine.reactorType());
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
        assertTrueEventually(() -> assertEquals(TERMINATED, engine.state()));
    }

    @Test
    public void shutdown_whenRunning() throws InterruptedException {
        engine = new TpcEngine();
        engine.start();
        engine.reactor(0).offer(() -> {
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
        engine.reactor(0).offer(() -> {
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
        assertTrueEventually(() -> assertEquals(TERMINATED, engine.state()));
    }
}
