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

package com.hazelcast.jet;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.archunit.ArchUnitRules;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.archunit.ModuleImportOptions.onlyCurrentModule;

@RunWith(HazelcastParallelClassRunner.class)
@Category(ParallelJVMTest.class)
public class JetSerializableTest {
    @Test
    public void serializable_classes_should_have_valid_serialVersionUID() {
        String basePackage = JetService.class.getPackage().getName();
        JavaClasses classes = new ClassFileImporter()
                .withImportOption(onlyCurrentModule())
                .importPackages(basePackage);

        ArchUnitRules.SERIALIZABLE_SHOULD_HAVE_VALID_SERIAL_VERSION_UID.check(classes);
    }
}
