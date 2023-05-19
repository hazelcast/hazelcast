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

package com.hazelcast.jet.impl.submitjob.memberside.validator;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.impl.submitjob.memberside.JobMetaDataParameterObject;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JarOnMemberValidatorTest {

    @Test
    public void testValidateJarOnMember() {
        JobMetaDataParameterObject parameterObject = new JobMetaDataParameterObject();
        assertThatThrownBy(() -> JarOnMemberValidator.validate(parameterObject))
                .isInstanceOf(JetException.class);
    }

    @Test
    public void testValidateJarPathNotNull() {
        assertThatThrownBy(() -> JarOnMemberValidator.validateJarPathNotNull(null))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("File path can not be null");
    }

    @Test
    public void testValidateFileExtension() {
        Path jarPath = Paths.get("foo");
        assertThatThrownBy(() -> JarOnMemberValidator.validateFileExtension(jarPath))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("File name extension should be .jar");
    }

    @Test
    public void testValidateFileSizeIsNotZero() {
        Path jarPath = Paths.get("foo");
        assertThatThrownBy(() -> JarOnMemberValidator.validateFileSizeIsNotZero(jarPath))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("File is not accessible");
    }

    @Test
    public void testValidateJobParameters() {
        assertThatThrownBy(() -> JarOnMemberValidator.validateJobParameters(null))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("jobParameters can not be null");
    }
}
