/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.hadoop;

import com.hazelcast.test.archunit.ArchUnitRules;
import com.hazelcast.test.archunit.ArchUnitTestSupport;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.Test;

import static com.hazelcast.test.archunit.ModuleImportOptions.onlyCurrentModuleTests;

public class NoMixedJUnitAnnotationsInOurTestSourcesTest extends ArchUnitTestSupport {

    @Test
    public void noJUnitMixing() {
        String basePackage = "com.hazelcast";
        JavaClasses classes = new ClassFileImporter()
                .withImportOption(onlyCurrentModuleTests())
                .importPackages(basePackage);

        ArchUnitRules.NO_JUNIT_MIXING.check(classes);
    }
}
