/*
 * Copyright 2026 Hazelcast Inc.
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

package com.hazelcast;

import com.hazelcast.test.archunit.ArchUnitRules;
import com.hazelcast.test.archunit.ArchUnitTestSupport;
import com.hazelcast.test.archunit.ModuleImportOptions;
import com.tngtech.archunit.core.domain.JavaClasses;
import org.junit.Test;

public class ParallelJvmTestsDoNotUseNetworkTest extends ArchUnitTestSupport {

    @Test
    public void testDoNotUseNetwork() {
        String basePackage = "com.hazelcast";
        JavaClasses classes = ModuleImportOptions.getCurrentModuleTestClasses(basePackage);

        ArchUnitRules.PARALLEL_JVM_TESTS_MUST_NOT_CREATE_HAZELCAST_INSTANCES_WITH_NETWORK.check(classes);
    }
}
