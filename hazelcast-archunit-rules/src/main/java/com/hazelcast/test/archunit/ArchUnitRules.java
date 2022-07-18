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

package com.hazelcast.test.archunit;

import com.tngtech.archunit.base.Optional;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaField;
import com.tngtech.archunit.core.domain.JavaMethodCall;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.domain.JavaType;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.hazelcast.test.archunit.ArchUnitRules.CompletableFutureAsyncActionsWithExecutorOnlyCondition
        .useExplicitExecutorServiceInCFAsyncMethods;
import static com.hazelcast.test.archunit.ArchUnitRules.SerialVersionUidFieldCondition.haveValidSerialVersionUid;
import static com.tngtech.archunit.lang.SimpleConditionEvent.violated;
import static com.tngtech.archunit.lang.conditions.ArchConditions.beFinal;
import static com.tngtech.archunit.lang.conditions.ArchConditions.beStatic;
import static com.tngtech.archunit.lang.conditions.ArchConditions.haveRawType;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;


public final class ArchUnitRules {
    /**
     * ArchUnit rule checking that Serializable classes have a valid serialVersionUID
     */
    public static final ArchRule SERIALIZABLE_SHOULD_HAVE_VALID_SERIAL_VERSION_UID = classes()
            .that()
            .areNotEnums()
            .and().doNotHaveModifier(JavaModifier.ABSTRACT)
            .and().implement(Serializable.class)
            .and().doNotImplement("com.hazelcast.nio.serialization.DataSerializable")
            .and().areNotAnonymousClasses()
            .should(haveValidSerialVersionUid());

    /**
     * ArchUnit rule checking that {@link CompletableFuture} {@code async} methods are not used without
     * explicit executor service.
     */
    public static final ArchRule COMPLETABLE_FUTURE_ASYNC_USED_ONLY_WITH_EXPLICIT_EXECUTOR = classes()
            .that().areNotAssignableTo(CompletableFuture.class)
            .should(useExplicitExecutorServiceInCFAsyncMethods());

    private ArchUnitRules() {
    }

    static class SerialVersionUidFieldCondition extends ArchCondition<JavaClass> {
        private static final String FIELD_NAME = "serialVersionUID";

        SerialVersionUidFieldCondition() {
            super("have a valid " + FIELD_NAME);
        }

        @Override
        public void check(JavaClass clazz, ConditionEvents events) {
            Optional<JavaField> field = clazz.tryGetField(FIELD_NAME);
            if (field.isPresent()) {
                haveRawType("long").and(beFinal()).and(beStatic()).check(field.get(), events);
            } else {
                events.add(violated(clazz, FIELD_NAME + " field is missing in class " + clazz.getName()));
            }
        }

        static SerialVersionUidFieldCondition haveValidSerialVersionUid() {
            return new SerialVersionUidFieldCondition();
        }
    }

    static class CompletableFutureAsyncActionsWithExecutorOnlyCondition extends ArchCondition<JavaClass> {

        CompletableFutureAsyncActionsWithExecutorOnlyCondition() {
            super("use CompletableFuture async actions only with explicit executor service");
        }

        @Override
        public void check(JavaClass item, ConditionEvents events) {
            for (JavaMethodCall methodCalled : item.getMethodCallsFromSelf()) {
                String calledMethodName = methodCalled.getTarget().getName();
                if (isFromCompletableFuture(methodCalled) && calledMethodName.endsWith("Async")) {
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
            return calledClass.isAssignableFrom(CompletableFuture.class)
                    && !calledClass.isAssignableFrom("com.hazelcast.spi.impl.InternalCompletableFuture");
        }

        private boolean withoutExecutorArgument(List<JavaType> parameterTypes) {
            return parameterTypes.isEmpty()
                    || !parameterTypes.get(parameterTypes.size() - 1).toErasure().isEquivalentTo(Executor.class);
        }

        static ArchCondition<? super JavaClass> useExplicitExecutorServiceInCFAsyncMethods() {
            return new CompletableFutureAsyncActionsWithExecutorOnlyCondition();
        }
    }
}
