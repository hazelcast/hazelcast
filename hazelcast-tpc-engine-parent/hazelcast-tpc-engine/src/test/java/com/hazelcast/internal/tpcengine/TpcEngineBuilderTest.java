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

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

public class TpcEngineBuilderTest {

    @Test
    public void test_reactorConfigureFn_WhenNull() {
        TpcEngine.Builder builder = new TpcEngine.Builder();
        builder.reactorConfigureFn = null;
        assertThrows(NullPointerException.class, () -> builder.conclude());
    }

    @Test
    public void test_reactorCount_whenZero() {
        TpcEngine.Builder builder = new TpcEngine.Builder();
        builder.reactorCount = 0;
        assertThrows(IllegalArgumentException.class, () -> builder.conclude());
    }

    @Test
    public void test_reactorCount_whenNegative() {
        TpcEngine.Builder builder = new TpcEngine.Builder();
        builder.reactorCount = -1;
        assertThrows(IllegalArgumentException.class, () -> builder.conclude());
    }
//
//    @Test
//    public void test_setReactorCount_whenAlreadyBuilt() {
//        TpcEngine.Context builder = new TpcEngine.Context();
//        builder.build();
//
//        assertThrows(IllegalStateException.class, () -> builder.setReactorCount(0));
//    }

    @Test
    public void test_build() {
        TpcEngine.Builder builder = new TpcEngine.Builder();
        builder.reactorCount = 2;
        TpcEngine engine = builder.build();
        assertNotNull(engine);
        assertEquals(ReactorType.NIO, engine.reactorType());
        assertEquals(2, engine.reactorCount());
    }

    @Test
    public void test_build_whenAlreadyBuilt() {
        TpcEngine.Builder builder = new TpcEngine.Builder();
        builder.build();

        assertThrows(IllegalStateException.class, builder::build);
    }
}
