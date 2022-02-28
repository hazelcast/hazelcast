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

/**
 * {@link ParametersRunnerFactory} implementation which creates {@link HazelcastParallelClassRunner}
 * to run test methods in parallel.
 * <p>
 * See {@link com.hazelcast.test package documentation} for runners overview.
 */
public class HazelcastParallelParametersRunnerFactory extends HazelcastParametersRunnerFactory {

    @Override
    protected Runner getClassRunner(Class<?> testClass, Object[] parameters, String testName)
            throws InitializationError {
        return new HazelcastParallelClassRunner(testClass, parameters, testName);
    }
}
