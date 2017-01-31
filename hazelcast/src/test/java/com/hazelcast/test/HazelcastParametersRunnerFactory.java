/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.test.annotation.ParallelTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.Runner;
import org.junit.runners.model.InitializationError;
import org.junit.runners.parameterized.ParametersRunnerFactory;
import org.junit.runners.parameterized.TestWithParameters;

/**
 * ParametersRunnerFactory implementation which creates either {@link HazelcastSerialClassRunner}
 * or {@link HazelcastParallelClassRunner}, depending on existence of {@link ParallelTest} category.
 *
 * When {@link ParallelTest} category is used, test class will be run with parallel method execution
 * either on a single JVM or on multiple JVMs depending on the maven profile selected.
 * Otherwise it will be run in full isolation (single JVM, serial method execution).
 */
public class HazelcastParametersRunnerFactory implements ParametersRunnerFactory {

    @Override
    public Runner createRunnerForTestWithParameters(TestWithParameters test) throws InitializationError {
        Class<?> testClass = test.getTestClass().getJavaClass();
        Object[] parameters = test.getParameters().toArray();
        String testName = test.getName();

        boolean isParallel = isParallel(testClass);
        if (isParallel) {
            return getParallelClassRunner(testClass, parameters, testName);
        }
        return getSerialClassRunner(testClass, parameters, testName);
    }

    private boolean isParallel(Class<?> testClass) {
        Category category = testClass.getAnnotation(Category.class);
        if (category == null) {
            return false;
        }

        Class<?>[] categories = category.value();
        for (Class<?> clazz : categories) {
            if (clazz == ParallelTest.class) {
                return true;
            }
        }
        return false;
    }

    // needs to be protected for Hazelcast Enterprise HazelcastParametersRunnerFactory
    protected HazelcastSerialClassRunner getSerialClassRunner(Class<?> testClass, Object[] parameters, String testName)
            throws InitializationError {
        return new HazelcastSerialClassRunner(testClass, parameters, testName);
    }

    // needs to be protected for Hazelcast Enterprise HazelcastParametersRunnerFactory
    protected HazelcastParallelClassRunner getParallelClassRunner(Class<?> testClass, Object[] parameters, String testName)
            throws InitializationError {
        return new HazelcastParallelClassRunner(testClass, parameters, testName);
    }
}
