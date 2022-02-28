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

package com.hazelcast.test;

import org.junit.runner.Runner;
import org.junit.runners.model.InitializationError;
import org.junit.runners.parameterized.ParametersRunnerFactory;
import org.junit.runners.parameterized.TestWithParameters;

/**
 * Abstract {@link ParametersRunnerFactory} superclass for {@link HazelcastSerialClassRunner}
 * and {@link HazelcastParallelClassRunner}.
 * <p>
 * See {@link com.hazelcast.test package documentation} for runners overview.
 */
public abstract class HazelcastParametersRunnerFactory implements ParametersRunnerFactory {

    @Override
    public Runner createRunnerForTestWithParameters(TestWithParameters test) throws InitializationError {
        Class<?> testClass = test.getTestClass().getJavaClass();
        Object[] parameters = test.getParameters().toArray();
        String testName = test.getName();

        return getClassRunner(testClass, parameters, testName);
    }

    protected abstract Runner getClassRunner(Class<?> testClass, Object[] parameters, String testName)
            throws InitializationError;
}
