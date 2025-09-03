/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.tpcengine.nio.NioReactorBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

public class TpcEngineBuilderTest {

    @Test
    public void test_setReactorBuilderWhenNull() {
        TpcEngineBuilder builder = new TpcEngineBuilder();
        assertThrows(NullPointerException.class, () -> builder.setReactorBuilder(null));
    }

    @Test
    public void test_setReactorCountWhenZero() {
        TpcEngineBuilder builder = new TpcEngineBuilder();
        assertThrows(IllegalArgumentException.class, () -> builder.setReactorCount(0));
    }

    @Test
    public void test_setReactorCountWhenNegative() {
        TpcEngineBuilder builder = new TpcEngineBuilder();
        assertThrows(IllegalArgumentException.class, () -> builder.setReactorCount(-1));
    }

    @Test
    public void test_build() {
        TpcEngine engine = new TpcEngineBuilder()
                .setReactorCount(2)
                .setReactorBuilder(new NioReactorBuilder())
                .build();
        assertNotNull(engine);
        assertEquals(ReactorType.NIO, engine.reactorType());
        assertEquals(2, engine.reactorCount());
    }
}
