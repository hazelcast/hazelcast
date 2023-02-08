/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.archunit;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaMember;
import com.tngtech.archunit.core.domain.JavaMethodCall;
import com.tngtech.archunit.core.domain.JavaType;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.tngtech.archunit.lang.SimpleConditionEvent.violated;

/**
 * <ul>
 * <li> from {@link CompletionStage} create a list of methods that have an {@code Async} counterpart
 * <li> based on this list, filtering all their calls on the {@link CompletableFuture} {@code instanceof} objects
 * <li> checking that no non-Async methods versions are used
 * <li> checking that for Async methods the {@link Executor} service is specified
 * <li> skipping methods that override {@link CompletableFuture} base class methods
 * </ul>
 */
public class CompletableFutureUsageCondition extends ArchCondition<JavaClass> {

    private static final Set<String> COMPLETION_STAGE_METHODS = new ClassFileImporter().importClass(CompletionStage.class)
            .getMethods().stream()
            .map(JavaMember::getName)
            .collect(Collectors.toSet());

    private static final Set<String> SYNC_AND_ASYNC_METHODS = collectSyncAndAsyncCounterpartMethods();

    CompletableFutureUsageCondition() {
        super("use only CompletableFuture async methods with explicit executor service");
    }

    private static Set<String> collectSyncAndAsyncCounterpartMethods() {
        return COMPLETION_STAGE_METHODS.stream()
                .flatMap(method -> {
                    if (method.endsWith("Async")) {
                        String syncMethod = method.substring(0, method.lastIndexOf(("Async")));
                        return COMPLETION_STAGE_METHODS.contains(syncMethod) ? Stream.of(syncMethod, method) : Stream.of(method);
                    } else {
                        return Stream.empty();
                    }
                })
                .collect(Collectors.toSet());
    }

    static ArchCondition<? super JavaClass> useExplicitExecutorServiceInCFAsyncMethods() {
        return new CompletableFutureUsageCondition();
    }

    public void check(JavaClass item, ConditionEvents events) {
        for (JavaMethodCall methodCalled : item.getMethodCallsFromSelf()) {
            String calledMethodName = methodCalled.getTarget().getName();
            if (isFromCompletableFuture(methodCalled)
                    && isAsyncMethod(calledMethodName)
                    && isNotOverrideCompletableFutureMethod(methodCalled, item)) {
                List<JavaType> parameterTypes = methodCalled.getTarget().getParameterTypes();
                if (withoutExecutorArgument(parameterTypes)) {
                    String violatingMethodName = methodCalled.getOwner().getFullName();
                    events.add(violated(item, violatingMethodName + ":" + methodCalled.getLineNumber()
                            + " calls CompletableFuture." + calledMethodName));
                }
            }
        }
    }

    private boolean isFromCompletableFuture(JavaMethodCall methodCalled) {
        JavaClass calledClass = methodCalled.getTarget().getOwner();
        return calledClass.isAssignableTo(CompletableFuture.class);
    }

    private boolean isNotOverrideCompletableFutureMethod(JavaMethodCall methodCalled, JavaClass item) {
        return !item.isAssignableTo(CompletableFuture.class)
                || !COMPLETION_STAGE_METHODS.contains(methodCalled.getOwner().getName());
    }

    private boolean isAsyncMethod(String methodName) {
        return SYNC_AND_ASYNC_METHODS.contains(methodName);
    }

    private boolean withoutExecutorArgument(List<JavaType> parameterTypes) {
        return parameterTypes.isEmpty()
                || !parameterTypes.get(parameterTypes.size() - 1).toErasure().isEquivalentTo(Executor.class);
    }
}
