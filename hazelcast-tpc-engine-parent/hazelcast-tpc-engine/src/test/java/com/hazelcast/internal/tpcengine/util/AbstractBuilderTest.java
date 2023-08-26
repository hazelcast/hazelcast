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

package com.hazelcast.internal.tpcengine.util;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class AbstractBuilderTest {

    @Test
    public void test_whenBuildTwice() {
        DummyBuilder builder = new DummyBuilder();
        builder.build();

        assertThrows(IllegalStateException.class, builder::build);
        assertFalse(builder.closeCalled);
    }

    @Test
    public void test_whenConcludeFails() {
        FailConcludeBuilder builder = new FailConcludeBuilder();

        assertThrows(IllegalArgumentException.class, builder::build);

        assertTrue(builder.closeCalled);
    }

    @Test
    public void test_whenConstructFails() {
        FailConstructBuilder builder = new FailConstructBuilder();

        assertThrows(IllegalArgumentException.class, builder::build);

        assertTrue(builder.closeCalled);
    }

    private class DummyBuilder extends AbstractBuilder {

        private boolean closeCalled;

        @Override
        public void close() throws Exception {
            closeCalled = true;
        }

        @Override
        protected Object construct() {
            return "banana";
        }
    }

    private class FailConcludeBuilder extends AbstractBuilder {

        private boolean closeCalled;


        @Override
        public void close() throws Exception {
            closeCalled = true;
        }

        @Override
        protected Object construct() {
            throw new IllegalArgumentException();
        }
    }

    private class FailConstructBuilder extends AbstractBuilder {

        private boolean closeCalled;

        @Override
        protected void conclude() {
            throw new IllegalArgumentException();
        }

        @Override
        public void close() throws Exception {
            closeCalled = true;
        }

        @Override
        protected Object construct() {
            return "banana";
        }
    }
}
