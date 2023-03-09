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

package com.hazelcast.jet.impl.submitjob.clientside.validator;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.impl.SubmitJobParameters;
import org.junit.Test;

import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.hazelcast.jet.impl.submitjob.clientside.upload.JobUploadClientFailureTest.getJarPath;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SubmitJobParametersValidatorTest {

    @Test
    public void failJarOnMemberConfiguration() {
        SubmitJobParameters parameterObject = SubmitJobParameters.withJarOnMember();
        assertThatThrownBy(() -> SubmitJobParametersValidator.validateJarOnClient(parameterObject))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("SubmitJobParameters is configured for jar on member");
    }

    @Test
    public void failJarOnClientConfiguration() {
        SubmitJobParameters parameterObject = SubmitJobParameters.withJarOnClient();
        assertThatThrownBy(() -> SubmitJobParametersValidator.validateJarOnMember(parameterObject))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("SubmitJobParameters is configured for jar on client");
    }

    @Test
    public void nullJarPath() {
        SubmitJobParameters parameterObject = SubmitJobParameters.withJarOnClient();
        assertThatThrownBy(() -> SubmitJobParametersValidator.validateJarOnClient(parameterObject))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("jarPath can not be null");
    }

    @Test
    public void noSuchFileException() {
        SubmitJobParameters parameterObject = SubmitJobParameters.withJarOnClient();
        parameterObject.setJarPath(Paths.get("nosuchfile.jar"));
        assertThatThrownBy(() -> SubmitJobParametersValidator.validateJarOnClient(parameterObject))
                .isInstanceOf(NoSuchFileException.class);
    }

    @Test
    public void invalidFileExtension() {
        Path jarPath1 = Paths.get("/mnt/foo");
        assertThatThrownBy(() -> SubmitJobParametersValidator.validateFileExtension(jarPath1))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("File name extension should be .jar");

        Path jarPath2 = Paths.get("foo");
        assertThatThrownBy(() -> SubmitJobParametersValidator.validateFileExtension(jarPath2))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("File name extension should be .jar");
    }

    @Test
    public void nullJobParameters() {
        SubmitJobParameters parameterObject = SubmitJobParameters.withJarOnClient();
        parameterObject.setJarPath(getJarPath());
        parameterObject.setJobParameters(null);
        assertThatThrownBy(() -> SubmitJobParametersValidator.validateJarOnClient(parameterObject))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("jobParameters can not be null");
    }
}
