/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.application;

import com.hazelcast.core.Member;
import com.hazelcast.jet.impl.hazelcast.InvocationFactory;
import com.hazelcast.jet.impl.statemachine.application.ApplicationEvent;
import com.hazelcast.jet.impl.application.localization.Chunk;
import com.hazelcast.jet.config.ApplicationConfig;

import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Abstract service to invoke operation's of Jet's lifecycle
 * It can be used either for client or for server
 *
 * @param <PayLoad> - type of invokers
 */
public interface ApplicationInvocationService<PayLoad> {
    /**
     * @return - member of JET cluster;
     */
    Set<Member> getMembers();

    /**
     * @return - invoker for interrupt operation;
     */
    PayLoad createInterruptInvoker();

    /**
     * @return - invoker for execute operation;
     */
    PayLoad createExecutionInvoker();

    /**
     * @return - invoker to work with accumulators;
     */
    PayLoad createAccumulatorsInvoker();

    /**
     * @return - invoker to finalize application;
     */
    PayLoad createFinalizationInvoker();

    /**
     * @return - invoker to accept application's localization;
     */
    PayLoad createAcceptedLocalizationInvoker();

    /**
     * @param chunk - chunk of byte-code;
     * @return - invoker to localize application;
     */
    PayLoad createLocalizationInvoker(Chunk chunk);

    /**
     * Invoker to send JET event;
     *
     * @param applicationEvent - JET event
     * @return - invoker
     */
    PayLoad createEventInvoker(ApplicationEvent applicationEvent);

    /**
     * Return invoker to init JET application
     *
     * @param config - application config
     * @return - invoker to init application
     */
    PayLoad createInitApplicationInvoker(ApplicationConfig config);

    /**
     * Creates invocation to be called on the corresponding member;
     *
     * @param member           - member where invocation should be executed;
     * @param operationFactory - factory for operations;
     * @param <T>              - type of the return value;
     * @return - Callable object for the corresponding invocation;
     */
    <T> Callable<T> createInvocation(Member member,
                                     InvocationFactory<PayLoad> operationFactory);
}
