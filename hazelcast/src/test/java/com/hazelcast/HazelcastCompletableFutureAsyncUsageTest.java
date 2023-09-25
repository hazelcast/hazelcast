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

package com.hazelcast;

import com.hazelcast.test.archunit.ArchUnitRules;
import com.hazelcast.test.archunit.ArchUnitTestSupport;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.Test;

import static com.hazelcast.test.archunit.ModuleImportOptions.onlyCurrentModule;
import static com.tngtech.archunit.base.DescribedPredicate.not;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.lang.conditions.ArchPredicates.are;
public class HazelcastCompletableFutureAsyncUsageTest extends ArchUnitTestSupport {

    @Test
    public void noClassUsesCompletableFuture() {
        String basePackage = "com.hazelcast";
        JavaClasses classes = new ClassFileImporter()
                .withImportOption(onlyCurrentModule())
                .importPackages(basePackage);

        JavaClasses javaClassesFiltered = classes
                .that(are(not(resideInAPackage("com.hazelcast.jet.."))));
        ArchUnitRules.COMPLETABLE_FUTURE_USED_ONLY_WITH_EXPLICIT_EXECUTOR.check(javaClassesFiltered);
    }
}
