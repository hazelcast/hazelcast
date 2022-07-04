/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.core.Processor;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

import static org.junit.Assert.fail;

public class LoadClassesIsolated extends AbstractProcessor {

    static volatile AssertionError assertionErrorInClose;
    private final boolean shouldComplete;
    private final List<String> onClasspath;
    private final List<String> notOnClasspath;

    LoadClassesIsolated(boolean shouldComplete) {
        this.shouldComplete = shouldComplete;
        onClasspath = new ArrayList<>();
        onClasspath.add("com.sample.pojo.person.Person$Appereance");
        notOnClasspath = new ArrayList<>();
        notOnClasspath.add("com.sample.pojo.car.Car");
    }

    LoadClassesIsolated(List<String> onClasspath, List<String> notOnClasspath, boolean shouldComplete) {
        this.onClasspath = onClasspath;
        this.notOnClasspath = notOnClasspath;
        this.shouldComplete = shouldComplete;
    }

    @Override
    protected void init(@Nonnull Processor.Context context) {
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
        for (String classname : onClasspath) {
            try {
                cl.loadClass(classname);
            } catch (ClassNotFoundException e) {
                fail(e.getMessage());
            }
        }
        for (String classname : notOnClasspath) {
            try {
                cl.loadClass(classname);
                fail();
            } catch (ClassNotFoundException ignored) {
            }
        }
    }
}
