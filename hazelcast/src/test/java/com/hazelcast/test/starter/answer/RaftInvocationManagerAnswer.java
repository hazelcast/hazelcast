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

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.raft.QueryPolicy;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.spi.impl.operationservice.OperationService;
import org.mockito.invocation.InvocationOnMock;

import java.lang.reflect.Method;

import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyArgumentsIfNeeded;
import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyObjectForStarter;
import static org.mockito.Mockito.mock;

/**
 * Default {@link org.mockito.stubbing.Answer} to create a mock for a proxied
 * {@link OperationService}.
 * <p>
 * This class uses (de)serialization to transfer Hazelcast operations from the
 * test classloader to the delegate classloader.
 */
class RaftInvocationManagerAnswer
        extends AbstractAnswer {

    private final SerializationService serializationService;
    private final Object delegateSerializationService;
    private final Method delegateToObjectMethod;
    private final Class<?> delegateOperationClass;
    private final Class<?> delegateCPGroupIdClass;
    private final Class<?> delegateQueryPolicyClass;

    RaftInvocationManagerAnswer(Object delegate, Object delegateSerializationService) throws Exception {
        super(delegate);
        serializationService = new DefaultSerializationServiceBuilder().build();
        this.delegateSerializationService = delegateSerializationService;
        delegateToObjectMethod = delegateSerializationService.getClass().getMethod("toObject", Object.class);
        delegateOperationClass = delegateClassloader.loadClass(RaftOp.class.getName());
        delegateCPGroupIdClass = delegateClassloader.loadClass(CPGroupId.class.getName());
        delegateQueryPolicyClass = delegateClassloader.loadClass(QueryPolicy.class.getName());
    }

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
        String methodName = invocation.getMethod().getName();
        Object[] arguments = invocation.getArguments();
        Class[] argumentClasses = new Class[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            if (arguments[i] instanceof RaftOp) {
                // transfer the operation to the delegateClassloader via (de)serialization
                Object dataOperation = serializationService.toData(arguments[i]);
                Object delegateDataOperation = proxyObjectForStarter(delegateClassloader, dataOperation);
                Object delegateOperation = delegateToObjectMethod.invoke(delegateSerializationService, delegateDataOperation);
                arguments[i] = delegateOperationClass.cast(delegateOperation);
                argumentClasses[i] = delegateOperationClass;
            } else if (arguments[i] instanceof Integer) {
                argumentClasses[i] = Integer.TYPE;
            } else if (arguments[i] instanceof RaftGroupId) {
                argumentClasses[i] = delegateCPGroupIdClass;
            } else if (arguments[i] instanceof QueryPolicy) {
                  argumentClasses[i] = delegateQueryPolicyClass;
            } else {
                argumentClasses[i] = arguments[i].getClass();
            }
        }
        Method delegateMethod = getDelegateMethod(methodName, argumentClasses);
        Object[] proxiedArguments = proxyArgumentsIfNeeded(arguments, delegateClassloader);
        Object result = invoke(false, delegateMethod, proxiedArguments);
        if (invocation.getMethod().getName().equals("invoke")) {
            return mock(invocation.getMethod().getReturnType(), new DelegatingAnswer(result));
        } else {
            return proxyObjectForStarter(targetClassloader, result);
        }
    }

    @Override
    Object answer(InvocationOnMock invocation, String methodName, Object[] arguments) {
        throw new UnsupportedOperationException();
    }
}
