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

package com.hazelcast.jet.impl;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.SubmitJobParameters;
import org.junit.Test;

import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;

import static com.hazelcast.jet.core.JobUploadClientFailureTest.getJarPath;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SubmitJobParametersValidatorTest {

    @Test
    public void nullJarPath() {
        SubmitJobParametersValidator validator = new SubmitJobParametersValidator();

        SubmitJobParameters parameterObject = new SubmitJobParameters();
        assertThatThrownBy(() -> validator.validateParameterObject(parameterObject))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("jarPath can not be null");
    }

    @Test
    public void noSuchFileException() {
        SubmitJobParametersValidator validator = new SubmitJobParametersValidator();

        SubmitJobParameters parameterObject = new SubmitJobParameters();
        parameterObject.setJarPath(Paths.get("nosuchfile.jar"));
        assertThatThrownBy(() -> validator.validateParameterObject(parameterObject))
                .isInstanceOf(NoSuchFileException.class);
    }

    @Test
    public void nullJobParameters() {
        SubmitJobParametersValidator validator = new SubmitJobParametersValidator();

        SubmitJobParameters parameterObject = new SubmitJobParameters();
        parameterObject.setJarPath(getJarPath());
        parameterObject.setJobParameters(null);
        assertThatThrownBy(() -> validator.validateParameterObject(parameterObject))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("jobParameters can not be null");
    }
}
