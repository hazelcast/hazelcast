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
import java.util.Properties;

import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;

@HazelcastStarterConstructor(classNames = {"com.hazelcast.spi.properties.HazelcastProperties"})
public class HazelcastPropertiesConstructor extends AbstractStarterObjectConstructor {

    public HazelcastPropertiesConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate) throws Exception {
        // obtain reference to constructor HazelcastProperties(Properties nullableProperties)
        Constructor<?> constructor = targetClass.getDeclaredConstructor(Properties.class);

        Properties properties = (Properties) getFieldValueReflectively(delegate, "properties");
        Object[] args = new Object[]{properties};

        return constructor.newInstance(args);
    }
}
