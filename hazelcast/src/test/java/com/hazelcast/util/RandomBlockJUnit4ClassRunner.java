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

import com.hazelcast.impl.GroupProperties;
import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Run the tests randomly and log the running test.
 */
public class RandomBlockJUnit4ClassRunner extends BlockJUnit4ClassRunner {

    static {
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
    }

    final static String indexStr = System.getProperty("hazelcast.test.index");
    final static String concurrencyLevelStr = System.getProperty("hazelcast.test.concurrency.level");
    final static AtomicInteger testCounter = new AtomicInteger();

    public RandomBlockJUnit4ClassRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }

    protected List<FrameworkMethod> computeTestMethods() {
        List<FrameworkMethod> methods = super.computeTestMethods();
        if (indexStr != null && concurrencyLevelStr != null) {
            int index = Integer.parseInt(indexStr);
            System.setProperty("hazelcast.multicast.group", "224.2.2." + (10 + index));
            int concurrencyLevel = Integer.parseInt(concurrencyLevelStr);
            List<FrameworkMethod> filteredMethods = new ArrayList<FrameworkMethod>(methods.size());
            for (FrameworkMethod method : methods) {
                String methodName = method.getName();
                String name = methodName.toLowerCase().replaceAll("test", "");
                if (Math.abs(name.hashCode()) % concurrencyLevel == index) {
                    filteredMethods.add(method);
                }
            }
            methods = filteredMethods;
            int total = testCounter.addAndGet(filteredMethods.size());
            System.out.println("PLOG: " + index + "/" + concurrencyLevel + " will run " + total + " tests.");
        }
        Collections.shuffle(methods);
        return methods;
    }

    //
    protected void validateInstanceMethods(List<Throwable> errors) {
        if (indexStr == null || concurrencyLevelStr == null) {
            super.validateInstanceMethods(errors);
        }
    }

    @Override
    protected void runChild(FrameworkMethod method, RunNotifier notifier) {
        long start = System.currentTimeMillis();
        String testName = method.getMethod().getDeclaringClass().getSimpleName() + "." + method.getName();
        notifier.addListener(new RunListener() {
            @Override
            public void testRunFinished(Result result) throws Exception {
                super.testRunFinished(result);
            }
        });
        System.out.println("Started Running Test: " + testName);
        super.runChild(method, notifier);
        long took = (System.currentTimeMillis() - start) / 1000;
        System.out.println(String.format("Finished Running Test: %s in %d seconds.", testName, took));
    }
}