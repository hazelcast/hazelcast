/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.starter;

import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ExceptionUtil;

/**
 * Abstract superclass for {@code ConstructorFunction}s which, given a target {@code Class}, create an {@code Object} of
 * target {@code Class} off an input {@code Object}. For example, assuming a {@code Config} instance in current classloader,
 * the appropriate {@code ConstructorFunction} would create a {@code Config} object representing the same configuration for
 * the classloader that loads Hazelcast version 3.8.
 */
public abstract class AbstractStarterObjectConstructor implements ConstructorFunction<Object, Object> {

    protected final Class<?> targetClass;

    public AbstractStarterObjectConstructor(Class<?> targetClass) {
        this.targetClass = targetClass;
    }

    @Override
    public Object createNew(Object arg) {
        try {
            return createNew0(arg);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    abstract Object createNew0(Object delegate) throws Exception;

}
