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

package com.hazelcast.test.starter.constructor;

import com.hazelcast.test.starter.HazelcastStarterConstructor;

import java.lang.reflect.Constructor;

import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyArgumentsIfNeeded;
import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;

@HazelcastStarterConstructor(classNames = {"com.hazelcast.query.impl.predicates.GreaterLessPredicate"})
public class GreaterLessPredicateConstructor extends AbstractStarterObjectConstructor {

    public GreaterLessPredicateConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate)
            throws Exception {
        ClassLoader starterClassLoader = targetClass.getClassLoader();
        Constructor<?> constructor = targetClass.getDeclaredConstructor(String.class,
                Comparable.class, Boolean.TYPE, Boolean.TYPE);

        Object attributeName = getFieldValueReflectively(delegate, "attributeName");
        Object value = getFieldValueReflectively(delegate, "value");
        Object equal = getFieldValueReflectively(delegate, "equal");
        Object less = getFieldValueReflectively(delegate, "less");
        Object[] args = new Object[]{attributeName, value, equal, less};
        Object[] proxiedArgs = proxyArgumentsIfNeeded(args, starterClassLoader);

        return constructor.newInstance(proxiedArgs);
    }
}
