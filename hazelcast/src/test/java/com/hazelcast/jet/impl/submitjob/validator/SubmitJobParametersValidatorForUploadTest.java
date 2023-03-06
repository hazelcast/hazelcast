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

package com.hazelcast.jet.impl.submitjob.validator;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.impl.SubmitJobParameters;
import org.junit.Test;

import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.hazelcast.jet.impl.submitjob.clientside.upload.JobUploadClientFailureTest.getJarPath;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SubmitJobParametersValidatorForUploadTest {

    @Test
    public void failForDirectJobExecution() {
        SubmitJobParametersValidator validator = new SubmitJobParametersValidator();

        SubmitJobParameters parameterObject = SubmitJobParameters.forDirectJobExecution();
        assertThatThrownBy(() -> validator.validateForJobUpload(parameterObject))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("SubmitJobParameters is configured for direct job execution");
    }

    @Test
    public void nullJarPath() {
        SubmitJobParametersValidator validator = new SubmitJobParametersValidator();

        SubmitJobParameters parameterObject = SubmitJobParameters.forJobUpload();
        assertThatThrownBy(() -> validator.validateForJobUpload(parameterObject))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("jarPath can not be null");
    }

    @Test
    public void noSuchFileException() {
        SubmitJobParametersValidator validator = new SubmitJobParametersValidator();

        SubmitJobParameters parameterObject = SubmitJobParameters.forJobUpload();
        parameterObject.setJarPath(Paths.get("nosuchfile.jar"));
        assertThatThrownBy(() -> validator.validateForJobUpload(parameterObject))
                .isInstanceOf(NoSuchFileException.class);
    }

    @Test
    public void invalidFileExtension() {
        SubmitJobParametersValidator validator = new SubmitJobParametersValidator();

        Path jarPath1 = Paths.get("/mnt/foo");
        assertThatThrownBy(() -> validator.validateFileExtension(jarPath1))
                .isInstanceOf(JetException.class);

        Path jarPath2 = Paths.get("foo");
        assertThatThrownBy(() -> validator.validateFileExtension(jarPath2))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("File name extension should be .jar");
    }

    @Test
    public void nullJobParameters() {
        SubmitJobParametersValidator validator = new SubmitJobParametersValidator();

        SubmitJobParameters parameterObject = SubmitJobParameters.forJobUpload();
        parameterObject.setJarPath(getJarPath());
        parameterObject.setJobParameters(null);
        assertThatThrownBy(() -> validator.validateForJobUpload(parameterObject))
                .isInstanceOf(JetException.class)
                .hasMessageContaining("jobParameters can not be null");
    }
}
