/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.hazelcast.test;

import com.hazelcast.test.annotation.RunParallel;
import org.junit.runner.Runner;
import org.junit.runners.Suite;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.runners.Parameterized.Parameters;

/**
 * A testsuite with an ability to run parameterized tests.
 * <p/>
 * This {@link Suite} runs the tests with either {@link HazelcastSerialClassRunner} or {@link HazelcastParallelClassRunner}.
 */
public class HazelcastTestRunner extends Suite {

    private static final List<Runner> NO_RUNNERS = Collections.emptyList();

    private final ArrayList<Runner> runners = new ArrayList<Runner>();

    private boolean isParallel;

    public HazelcastTestRunner(Class<?> clazz) throws Throwable {
        super(clazz, NO_RUNNERS);

        RunParallel parallel = getTestClass().getJavaClass().getAnnotation(RunParallel.class);
        if (parallel != null) {
            isParallel = true;
        }
        Parameters parameters = getParametersMethod().getAnnotation(Parameters.class);
        createRunnersForParameters(allParameters(), parameters.name());
    }

    @Override
    protected List<Runner> getChildren() {
        return runners;
    }

    @SuppressWarnings("unchecked")
    private Iterable<Object[]> allParameters() throws Throwable {
        Object parameters = getParametersMethod().invokeExplosively(null);
        if (parameters instanceof Iterable) {
            return (Iterable<Object[]>) parameters;
        } else {
            throw parametersMethodReturnedWrongType();
        }
    }

    private FrameworkMethod getParametersMethod() throws Exception {
        List<FrameworkMethod> methods = getTestClass().getAnnotatedMethods(Parameters.class);
        for (FrameworkMethod each : methods) {
            if (each.isStatic() && each.isPublic()) {
                return each;
            }
        }
        throw new Exception("No public static parameters method on class " + getTestClass().getName());
    }

    private void createRunnersForParameters(Iterable<Object[]> allParameters, String namePattern) throws Exception {
        try {
            int i = 0;
            for (Object[] parametersOfSingleTest : allParameters) {
                String name = nameFor(namePattern, i, parametersOfSingleTest);
                AbstractHazelcastClassRunner runner;
                if (isParallel) {
                    runner = createParallelRunner(parametersOfSingleTest, name);
                } else {
                    runner = createSerialRunner(parametersOfSingleTest, name);
                }
                runner.setParameterized(true);
                runners.add(runner);
                ++i;
            }
        } catch (ClassCastException e) {
            throw parametersMethodReturnedWrongType();
        }
    }

    private String nameFor(String namePattern, int index, Object[] parameters) {
        String finalPattern = namePattern.replaceAll("\\{index\\}", Integer.toString(index));
        String name = MessageFormat.format(finalPattern, parameters);
        return "[" + name + "]";
    }

    private Exception parametersMethodReturnedWrongType() throws Exception {
        String className = getTestClass().getName();
        String methodName = getParametersMethod().getName();
        String message = MessageFormat.format("{0}.{1}() must return an Iterable of arrays.", className, methodName);
        return new Exception(message);
    }

    protected HazelcastParallelClassRunner createParallelRunner(Object[] parametersOfSingleTest, String name) throws InitializationError {
        return new HazelcastParallelClassRunner(getTestClass().getJavaClass(), parametersOfSingleTest, name);
    }

    protected HazelcastSerialClassRunner createSerialRunner(Object[] parametersOfSingleTest, String name) throws InitializationError {
        return new HazelcastSerialClassRunner(getTestClass().getJavaClass(), parametersOfSingleTest, name);
    }
}
