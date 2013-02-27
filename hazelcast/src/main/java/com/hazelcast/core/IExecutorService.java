/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

import com.hazelcast.monitor.LocalExecutorStats;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public interface IExecutorService extends ExecutorService, DistributedObject {

    void executeOnKeyOwner(Runnable command, Object key);

    void executeOnMember(Runnable command, Member member);

    void executeOnMembers(Runnable command, Collection<Member> members);

    void executeOnAllMembers(Runnable command);

    <T> Future<T> submitToKeyOwner(Callable<T> task, Object key);

    <T> Future<T> submitToMember(Callable<T> task, Member member);

    <T> Map<Member, Future<T>> submitToMembers(Callable<T> task, Collection<Member> members);

    <T> Map<Member, Future<T>> submitToAllMembers(Callable<T> task);

    void submit(Runnable task, ExecutionCallback callback);

    void submitToKeyOwner(Runnable task, Object key, ExecutionCallback callback);

    void submitToMember(Runnable task, Member member, ExecutionCallback callback);

    void submitToMembers(Runnable task, Collection<Member> members, MultiExecutionCallback callback);

    void submitToAllMembers(Runnable task, MultiExecutionCallback callback);

    <T> void submit(Callable<T> task, ExecutionCallback<T> callback);

    <T> void submitToKeyOwner(Callable<T> task, Object key, ExecutionCallback<T> callback);

    <T> void submitToMember(Callable<T> task, Member member, ExecutionCallback<T> callback);

    <T> void submitToMembers(Callable<T> task, Collection<Member> members, MultiExecutionCallback callback);

    <T> void submitToAllMembers(Callable<T> task, MultiExecutionCallback callback);

    LocalExecutorStats getLocalExecutorStats();
}
