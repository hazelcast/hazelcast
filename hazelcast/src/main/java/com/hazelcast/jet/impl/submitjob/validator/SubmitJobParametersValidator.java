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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

public class SubmitJobParametersValidator {

    public void validateForJobUpload(SubmitJobParameters parameterObject) throws IOException {
        if (parameterObject.isDirectJobExecution()) {
            throw new JetException("SubmitJobParameters is configured for direct job execution");
        }

        Path jarPath = parameterObject.getJarPath();
        validateJarPathNotNull(jarPath);
        validateFileSizeIsNotZero(jarPath);
        validateFileExtension(jarPath);
        validateJobParameters(parameterObject.getJobParameters());
    }

    public void validateForDirectJobExecution(SubmitJobParameters parameterObject) {
        if (!parameterObject.isDirectJobExecution()) {
            throw new JetException("SubmitJobParameters is configured for job upload");
        }

        Path jarPath = parameterObject.getJarPath();
        validateJarPathNotNull(jarPath);
        validateFileExtension(jarPath);
        validateJobParameters(parameterObject.getJobParameters());
    }

    void validateJarPathNotNull(Path jarPath) {
        // Check that parameter is not null, because it is used to access the file
        if (Objects.isNull(jarPath)) {
            throw new JetException("jarPath can not be null");
        }
    }

    void validateFileExtension(Path jarPath) {
        String fileName = jarPath.getFileName().toString();
        if (!fileName.endsWith(".jar")) {
            throw new JetException("File name extension should be .jar");
        }
    }

    void validateFileSizeIsNotZero(Path jarPath) throws IOException {
        // Check that the file exists and its size is not 0
        long jarSize = Files.size(jarPath);
        if (jarSize == 0) {
            throw new JetException("Jar size can not be 0");
        }
    }

    void validateJobParameters(List<String> jobParameters) {
        // Check that parameter is not null, because it is used by the JetUploadJobMetaDataCodec
        if (Objects.isNull(jobParameters)) {
            throw new JetException("jobParameters can not be null");
        }
    }
}
