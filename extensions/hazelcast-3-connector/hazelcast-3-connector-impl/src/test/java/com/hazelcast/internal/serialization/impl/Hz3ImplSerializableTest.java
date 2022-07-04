/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.connector.map.impl.MapReader;
import com.hazelcast.test.archunit.ArchUnitRules;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.Test;

import static com.hazelcast.test.archunit.ModuleImportOptions.onlyCurrentModule;

public class Hz3ImplSerializableTest {

    @Test
    public void serializable_classes_should_have_valid_serialVersionUID() {
        String basePackage = MapReader.class.getPackage().getName();
        JavaClasses classes = new ClassFileImporter()
                .withImportOption(onlyCurrentModule())
                .importPackages(basePackage);

        ArchUnitRules.SERIALIZABLE_SHOULD_HAVE_VALID_SERIAL_VERSION_UID.check(classes);
    }

}
