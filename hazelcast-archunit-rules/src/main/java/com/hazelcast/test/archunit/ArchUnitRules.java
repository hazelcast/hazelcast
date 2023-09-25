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

import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.lang.ArchRule;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.test.archunit.CompletableFutureUsageCondition.useExplicitExecutorServiceInCFAsyncMethods;
import static com.hazelcast.test.archunit.MatchersUsageCondition.notUseHamcrestMatchers;
import static com.hazelcast.test.archunit.MixTestAnnotationsCondition.notMixJUnit4AndJUnit5Annotations;
import static com.hazelcast.test.archunit.SerialVersionUidFieldCondition.haveValidSerialVersionUid;
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
            .should(haveValidSerialVersionUid())
            .allowEmptyShould(true);

    /**
     * ArchUnit rule checking that only {@link CompletableFuture} {@code async} methods version with explicitly
     * defined executor service is used.
     */
    public static final ArchRule COMPLETABLE_FUTURE_USED_ONLY_WITH_EXPLICIT_EXECUTOR = classes()
            .should(useExplicitExecutorServiceInCFAsyncMethods());

    /**
     * ArchUnit rule checking that Hamcrest matchers are not mixed with AssertJ.
     */
    public static final ArchRule MATCHERS_USAGE = classes()
            .that().haveSimpleNameEndingWith("Test")
            .should(notUseHamcrestMatchers());

    /**
     * ArchUnit rule checking that JUnit4 and JUnit5 annotations are not mixed within the same tes
     */
    public static final ArchRule NO_JUNIT_MIXING = classes()
            .that().haveSimpleNameEndingWith("Test")
            .should(notMixJUnit4AndJUnit5Annotations());

    private ArchUnitRules() {
    }

}
