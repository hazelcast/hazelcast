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

import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.Parameterized;
import org.junit.runners.model.FrameworkField;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

import java.lang.reflect.Field;
import java.util.List;

/**
 * A base test runner which has an ability to run parameterized tests.
 */
public abstract class AbstractParameterizedHazelcastClassRunner extends BlockJUnit4ClassRunner {

    protected boolean isParameterized;
    protected Object[] fParameters;
    protected String fName;

    /**
     * Creates a BlockJUnit4ClassRunner to run {@code clazz}
     *
     * @throws org.junit.runners.model.InitializationError if the test class is malformed.
     */
    public AbstractParameterizedHazelcastClassRunner(Class<?> clazz) throws InitializationError {
        super(clazz);
    }

    public AbstractParameterizedHazelcastClassRunner(Class<?> clazz, Object[] parameters, String name)
            throws InitializationError {
        super(clazz);
        fParameters = parameters;
        fName = name;
        isParameterized = true;
    }

    @Override
    protected String getName() {
        if (isParameterized) {
            return fName;
        } else {
            return super.getName();
        }
    }

    @Override
    protected String testName(FrameworkMethod method) {
        if (isParameterized) {
            return method.getName() + getName();
        } else {
            return method.getName();
        }
    }

    public void setParameterized(boolean isParameterized) {
        this.isParameterized = isParameterized;
    }

    @Override
    public Object createTest() throws Exception {
        if (isParameterized) {
            if (fieldsAreAnnotated()) {
                return createTestUsingFieldInjection();
            } else {
                return createTestUsingConstructorInjection();
            }
        }
        return super.createTest();
    }

    private Object createTestUsingConstructorInjection() throws Exception {
        return getTestClass().getOnlyConstructor().newInstance(fParameters);
    }

    @Override
    protected void validateConstructor(List<Throwable> errors) {
        validateOnlyOneConstructor(errors);
        if (fieldsAreAnnotated()) {
            validateZeroArgConstructor(errors);
        }
    }

    private Object createTestUsingFieldInjection() throws Exception {
        List<FrameworkField> annotatedFieldsByParameter = getAnnotatedFieldsByParameter();
        if (annotatedFieldsByParameter.size() != fParameters.length) {
            throw new Exception("Wrong number of parameters and @Parameter fields."
                    + " @Parameter fields counted: " + annotatedFieldsByParameter.size()
                    + ", available parameters: " + fParameters.length + ".");
        }
        Object testClassInstance = getTestClass().getJavaClass().newInstance();
        for (FrameworkField each : annotatedFieldsByParameter) {
            Field field = each.getField();
            Parameterized.Parameter annotation = field.getAnnotation(Parameterized.Parameter.class);
            int index = annotation.value();
            try {
                field.set(testClassInstance, fParameters[index]);
            } catch (IllegalArgumentException iare) {
                throw new Exception(getTestClass().getName() + ": Trying to set " + field.getName()
                        + " with the value " + fParameters[index]
                        + " that is not the right type (" + fParameters[index].getClass().getSimpleName() + " instead of "
                        + field.getType().getSimpleName() + ").", iare);
            }
        }
        return testClassInstance;
    }

    private boolean fieldsAreAnnotated() {
        return !getAnnotatedFieldsByParameter().isEmpty();
    }

    private List<FrameworkField> getAnnotatedFieldsByParameter() {
        return getTestClass().getAnnotatedFields(Parameterized.Parameter.class);
    }

    @Override
    protected Statement classBlock(RunNotifier notifier) {
        if (isParameterized) {
            return childrenInvoker(notifier);
        } else {
            return super.classBlock(notifier);
        }
    }

}
