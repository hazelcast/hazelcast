/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.util;

import org.junit.internal.runners.model.EachTestNotifier;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import java.util.Collections;
import java.util.List;

/**
 * Run the tests randomly and log the running test.
 */
public class RandomBlockJUnit4ClassRunner extends BlockJUnit4ClassRunner {

    public RandomBlockJUnit4ClassRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }

    @Override
    protected EachTestNotifier makeNotifier(FrameworkMethod method, RunNotifier notifier) {
        return super.makeNotifier(method, notifier);
    }

    protected List<FrameworkMethod> computeTestMethods() {
        List<FrameworkMethod> methods = super.computeTestMethods();
        Collections.shuffle(methods);
        return methods;
    }

    @Override
    protected void runChild(FrameworkMethod method, RunNotifier notifier) {
        long start = System.currentTimeMillis();
        String testName = method.getMethod().getDeclaringClass().getSimpleName() + "." + method.getName();
        System.out.println("Started Running Test: " + testName);
        super.runChild(method, notifier);
        long took = (System.currentTimeMillis() - start) / 1000;
        System.out.println(String.format("Finished Running Test: %s in %d seconds.", testName, took));
    }
}