/*
 * Copyright 2024 Hazelcast Inc.
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

package com.hazelcast.jet.avro;

import com.hazelcast.test.archunit.ArchUnitRules;
import com.hazelcast.test.archunit.ArchUnitTestSupport;
import com.hazelcast.test.archunit.ModuleImportOptions;
import com.tngtech.archunit.core.domain.JavaClasses;
import org.junit.Test;

public class TestsHaveRunnersTest extends ArchUnitTestSupport {

    @Test
    public void testHaveRunners() {
        String basePackage = "com.hazelcast";
        JavaClasses classes = ModuleImportOptions.getCurrentModuleTestClasses(basePackage);

        ArchUnitRules.TESTS_HAVE_RUNNNERS.check(classes);
    }
}
