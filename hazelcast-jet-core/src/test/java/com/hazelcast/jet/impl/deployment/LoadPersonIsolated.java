/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.deployment;

import com.hazelcast.jet.core.AbstractProcessor;

import javax.annotation.Nonnull;

import static org.junit.Assert.fail;

class LoadPersonIsolated extends AbstractProcessor {
    static volatile AssertionError assertionErrorInClose;
    private final boolean shouldComplete;

    LoadPersonIsolated(boolean shouldComplete) {
        this.shouldComplete = shouldComplete;
    }

    @Override
    protected void init(@Nonnull Context context) {
        checkLoadClass();
    }

    @Override
    public boolean complete() {
        checkLoadClass();
        return shouldComplete;
    }

    @Override
    public void close() {
        try {
            checkLoadClass();
        } catch (AssertionError e) {
            assertionErrorInClose = e;
        }
    }

    private void checkLoadClass() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            cl.loadClass("com.sample.pojo.person.Person$Appereance");
        } catch (ClassNotFoundException e) {
            fail(e.getMessage());
        }
        try {
            cl.loadClass("com.sample.pojo.car.Car");
            fail();
        } catch (ClassNotFoundException ignored) {
        }
    }
}
