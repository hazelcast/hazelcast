/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.wm.test;

import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.Parameterized;

import java.lang.reflect.Constructor;

public class WebTestRunner extends Runner {

    private Runner delegatedRunner;

    public WebTestRunner(Class<?> testClass) {
        DelegatedRunWith delegatedRunWith = testClass.getAnnotation(DelegatedRunWith.class);
        if (delegatedRunWith != null) {
            Class<? extends Runner> delegatedRunnerClass = delegatedRunWith.value();
            if (delegatedRunnerClass != null) {
                try {
                    Constructor<? extends Runner> delegatedRunnerClassConst =
                            delegatedRunnerClass.getConstructor(Class.class);
                    delegatedRunner = delegatedRunnerClassConst.newInstance(testClass);
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }
        if (delegatedRunner == null) {
            try {
                delegatedRunner = new Parameterized(testClass);
            } catch (Throwable t) {
                throw new RuntimeException("Unable to create delegated Test runner !", t);
            }
        }
    }

    @Override
    public Description getDescription() {
        return delegatedRunner.getDescription();
    }

    @Override
    public void run(RunNotifier notifier) {
        System.clearProperty(TestWebFilter.USE_SPRING_AWARE_FILTER_PROPERTY);
        delegatedRunner.run(notifier);

        System.setProperty(TestWebFilter.USE_SPRING_AWARE_FILTER_PROPERTY, "true");
        delegatedRunner.run(notifier);
    }
}
