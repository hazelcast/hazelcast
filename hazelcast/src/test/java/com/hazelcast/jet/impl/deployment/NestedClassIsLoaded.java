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

package com.hazelcast.jet.impl.deployment;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;

import javax.annotation.Nonnull;
import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class NestedClassIsLoaded extends AbstractProcessor {

    @Override
    protected void init(@Nonnull Processor.Context context) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            Class<?> clazz = cl.loadClass("com.sample.lambda.Worker$InnerWorker");
            Method method = clazz.getMethod("map", String.class);
            // We invoke the method so that the lambda inside of it is executed.
            // We check the lambda is loaded with the outer class.
            assertEquals("some-string", method.invoke(clazz.getDeclaredConstructor().newInstance(), "some-string"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
