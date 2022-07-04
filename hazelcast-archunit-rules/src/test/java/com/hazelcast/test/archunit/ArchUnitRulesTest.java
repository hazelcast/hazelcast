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

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.Test;

import static com.tngtech.archunit.core.importer.ImportOption.Predefined.ONLY_INCLUDE_TESTS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ArchUnitRulesTest {
    @Test
    public void should_fail_with_non_compliant_class() {
        JavaClasses classes = new ClassFileImporter()
                .withImportOption(ONLY_INCLUDE_TESTS)
                .importPackages("com.example.broken");
        assertThat(classes).isNotEmpty();

        assertThatThrownBy(() -> ArchUnitRules.SERIALIZABLE_SHOULD_HAVE_VALID_SERIAL_VERSION_UID.check(classes))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("was violated (1 times)");
    }

    @Test
    public void should_NOT_fail_with_non_compliant_class() {
        JavaClasses classes = new ClassFileImporter()
                .withImportOption(ONLY_INCLUDE_TESTS)
                .importPackages("com.example.valid");
        assertThat(classes).isNotEmpty();

        ArchUnitRules.SERIALIZABLE_SHOULD_HAVE_VALID_SERIAL_VERSION_UID.check(classes);
    }
}
