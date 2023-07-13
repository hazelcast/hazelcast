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

import org.junit.Test;

import static com.hazelcast.internal.tpcengine.ReactorBuilder.newReactorBuilder;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

public class TpcEngineBuilderTest {

    @Test
    public void test_setReactorBuilderFn_WhenNull() {
        TpcEngineBuilder builder = new TpcEngineBuilder();
        assertThrows(NullPointerException.class, () -> builder.setReactorBuilderFn(null));
    }

    @Test
    public void test_setReactorBuilderFn_whenAlreadyBuilt() {
        TpcEngineBuilder builder = new TpcEngineBuilder();
        builder.build();

        assertThrows(IllegalStateException.class, () -> builder.setReactorBuilderFn(() -> newReactorBuilder(ReactorType.NIO)));
    }

    @Test
    public void test_setReactorCount_whenZero() {
        TpcEngineBuilder builder = new TpcEngineBuilder();
        assertThrows(IllegalArgumentException.class, () -> builder.setReactorCount(0));
    }

    @Test
    public void test_setReactorCount_whenNegative() {
        TpcEngineBuilder builder = new TpcEngineBuilder();
        assertThrows(IllegalArgumentException.class, () -> builder.setReactorCount(-1));
    }

    @Test
    public void test_setReactorCount_whenAlreadyBuilt() {
        TpcEngineBuilder builder = new TpcEngineBuilder();
        builder.build();

        assertThrows(IllegalStateException.class, () -> builder.setReactorCount(0));
    }

    @Test
    public void test_build() {
        TpcEngine engine = new TpcEngineBuilder()
                .setReactorCount(2)
                .build();
        assertNotNull(engine);
        assertEquals(ReactorType.NIO, engine.reactorType());
        assertEquals(2, engine.reactorCount());
    }

    @Test
    public void test_build_whenAlreadyBuilt() {
        TpcEngineBuilder tpcEngineBuilder = new TpcEngineBuilder();
        tpcEngineBuilder.build();

        assertThrows(IllegalStateException.class, tpcEngineBuilder::build);
    }
}
