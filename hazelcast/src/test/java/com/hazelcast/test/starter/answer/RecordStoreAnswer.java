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

package com.hazelcast.test.starter.answer;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.serialization.Data;
import org.mockito.invocation.InvocationOnMock;

import javax.cache.expiry.ExpiryPolicy;
import java.lang.reflect.Method;
import java.util.Collection;

import static java.util.Arrays.copyOf;

/**
 * Default {@link org.mockito.stubbing.Answer} to create a mock for a proxied
 * {@link com.hazelcast.map.impl.recordstore.RecordStore} or
 * {@link com.hazelcast.cache.impl.ICacheRecordStore}.
 */
class RecordStoreAnswer extends AbstractAnswer {

    private final Class<?> delegateDataClass;
    private final Class<?> delegateAddressClass;
    private final Class<?> delegateExpiryPolicyClass;

    RecordStoreAnswer(Object delegate) throws Exception {
        super(delegate);
        delegateDataClass = delegateClassloader.loadClass(Data.class.getName());
        delegateAddressClass = delegateClassloader.loadClass(Address.class.getName());
        delegateExpiryPolicyClass = delegateClassloader.loadClass(ExpiryPolicy.class.getName());
    }

    @Override
    Object answer(InvocationOnMock invocation, String methodName, Object[] arguments) throws Exception {
        if (arguments.length == 3 && methodName.equals("get")) {
            // RecordStore
            return getMapValue(methodName, arguments);
        } else if (arguments.length == 3 && methodName.equals("setExpiryPolicy")) {
            // ICacheRecordStore
            return setExpiryPolicy(invocation, methodName, arguments);
        } else if (arguments.length == 2 && methodName.equals("get")) {
            // ICacheRecordStore
            return getCacheValue(methodName, arguments);
        } else if (arguments.length == 0 && methodName.equals("getReadOnlyRecords")) {
            // ICacheRecordStore
            return invoke(invocation);
        } else if (arguments.length == 0 && methodName.equals("size")) {
            return invoke(invocation);
        }
        throw new UnsupportedOperationException("Method is not implemented in RecordStoreAnswer: " + methodName);
    }

    private Object getMapValue(String methodName, Object[] arguments) throws Exception {
        // the RecordStore.get() method changed its signature between 3.10 and 3.11
        try {
            Method delegateMethod = getDelegateMethod(methodName, delegateDataClass, Boolean.TYPE, delegateAddressClass);
            return invoke(delegateMethod, arguments);
        } catch (NoSuchMethodException e) {
            Method delegateMethod = getDelegateMethod(methodName, delegateDataClass, Boolean.TYPE);
            return invoke(delegateMethod, arguments[0], arguments[1]);
        }
    }

    private Object getCacheValue(String methodName, Object[] arguments) throws Exception {
        Method delegateMethod = getDelegateMethod(methodName, delegateDataClass, delegateExpiryPolicyClass);
        return invoke(delegateMethod, arguments);
    }

    private Object setExpiryPolicy(InvocationOnMock invocation, String methodName, Object[] arguments) throws Exception {
        // the ICacheRecordStore.setExpiryPolicy() method changed its signature between 3.10 and 3.11
        try {
            return invoke(invocation, arguments);
        } catch (NoSuchMethodException e) {
            Method delegateMethod = getDelegateMethod(methodName, Collection.class, Object.class, String.class, Integer.TYPE);
            Object[] legacyArguments = copyOf(arguments, 4);
            legacyArguments[3] = 0;
            return invoke(delegateMethod, legacyArguments);
        }
    }
}
