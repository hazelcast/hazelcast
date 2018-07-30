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

package com.hazelcast.test.starter.constructor;

import com.hazelcast.test.starter.HazelcastStarterConstructor;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;

import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyArgumentsIfNeeded;
import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;

@HazelcastStarterConstructor(classNames = {"com.hazelcast.internal.partition.impl.InternalPartitionImpl"})
public class InternalPartitionImplConstructor extends AbstractStarterObjectConstructor {

    public InternalPartitionImplConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate) throws Exception {
        ClassLoader classloader = targetClass.getClassLoader();
        Class<?> partitionListenerClass = classloader.loadClass("com.hazelcast.internal.partition.PartitionListener");
        Class<?> addressClass = classloader.loadClass("com.hazelcast.nio.Address");
        Class<?> addressArrayClass = Array.newInstance(addressClass, 0).getClass();
        // obtain reference to constructor InternalPartitionImpl(int partitionId, PartitionListener listener,
        //                                                       Address thisAddress, Address[] addresses)
        Constructor<?> constructor = targetClass.getDeclaredConstructor(Integer.TYPE, partitionListenerClass, addressClass,
                addressArrayClass);

        Integer partitionId = (Integer) getFieldValueReflectively(delegate, "partitionId");
        Object partitionListener = getFieldValueReflectively(delegate, "partitionListener");
        Object thisAddress = getFieldValueReflectively(delegate, "thisAddress");
        Object addresses = getFieldValueReflectively(delegate, "addresses");
        Object[] args = new Object[]{partitionId, partitionListener, thisAddress, addresses};

        Object[] proxiedArgs = proxyArgumentsIfNeeded(args, classloader);
        return constructor.newInstance(proxiedArgs);
    }
}
