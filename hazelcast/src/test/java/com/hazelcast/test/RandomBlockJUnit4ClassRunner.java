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

import com.hazelcast.instance.GroupProperties;
import com.hazelcast.util.Clock;
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

    static {
        System.setProperty("hazelcast.logging.type", "log4j");
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
        System.setProperty("hazelcast.mancenter.enabled", "false");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
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
        long start = Clock.currentTimeMillis();
        String testName = method.getMethod().getDeclaringClass().getSimpleName() + "." + method.getName();
        System.out.println("Started Running Test: " + testName);
        super.runChild(method, notifier);
        float took = (float) (Clock.currentTimeMillis() - start) / 1000;
        System.out.println(String.format("Finished Running Test: %s in %.3f seconds.", testName, took));
    }
}
