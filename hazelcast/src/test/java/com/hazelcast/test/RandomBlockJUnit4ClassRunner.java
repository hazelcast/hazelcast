/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test;

import com.sun.management.OperatingSystemMXBean;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.List;

/**
 * Run the tests randomly and log the running test.
 */
public final class RandomBlockJUnit4ClassRunner extends BlockJUnit4ClassRunner {

    private static final OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    static {
        final String logging = "hazelcast.logging.type";
        if (System.getProperty(logging) == null) {
            System.setProperty(logging, "log4j");
        }
        if (System.getProperty(StaticNodeFactory.HAZELCAST_TEST_USE_NETWORK) == null) {
            System.setProperty(StaticNodeFactory.HAZELCAST_TEST_USE_NETWORK, "false");
        }
        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("hazelcast.mancenter.enabled", "false");
        System.setProperty("hazelcast.wait.seconds.before.join", "1");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        System.setProperty("java.net.preferIPv4Stack", "true");
    }


    public RandomBlockJUnit4ClassRunner(Class<?> klass) throws InitializationError {
        super(klass);
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
        long t0 = System.nanoTime();
        long cpu0 = osBean.getProcessCpuTime();
        super.runChild(method, notifier);
        long cpu1 = osBean.getProcessCpuTime();
        long t1 = System.nanoTime();
//        System.out.println(testName + "-> CPU-TIME: " + ((cpu1 - cpu0) / 1000 / 1000));
//        System.out.println(testName + "-> CPU-USAGE: " + Math.round((double) (cpu1 - cpu0) / (t1 - t0) * 100));
        float took = (float) (System.currentTimeMillis() - start) / 1000;
        System.out.println(String.format("Finished Running Test: %s in %.3f seconds.", testName, took));
    }
}
