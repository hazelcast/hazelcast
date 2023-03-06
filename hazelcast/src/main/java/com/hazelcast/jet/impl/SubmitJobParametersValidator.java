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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class SubmitJobParametersValidator {

    // Validate the parameters used by the client
    void validateParameterObject(SubmitJobParameters parameterObject) throws IOException {
        validateJar(parameterObject);

        validateJobParameters(parameterObject);
    }

    private void validateJar(SubmitJobParameters parameterObject) throws IOException {
        Path jarPath = parameterObject.getJarPath();

        // Check that parameter is not null, because it is used to access the file
        if (Objects.isNull(jarPath)) {
            throw new JetException("jarPath can not be null");
        }
        validateFileSize(jarPath);
        validateFileExtension(jarPath);
    }

    void validateFileExtension(Path jarPath) {
        String fileName = jarPath.getFileName().toString();
        if (!fileName.endsWith(".jar")) {
            throw new JetException("File name extension should be .jar");
        }
    }

    void validateFileSize(Path jarPath) throws IOException {
        // Check that the file exists and its size is not 0
        long jarSize = Files.size(jarPath);
        if (jarSize == 0) {
            throw new JetException("Jar size can not be 0");
        }
    }

    void validateJobParameters(SubmitJobParameters parameterObject) {
        // Check that parameter is not null, because it is used by the JetUploadJobMetaDataCodec
        if (Objects.isNull(parameterObject.getJobParameters())) {
            throw new JetException("jobParameters can not be null");
        }
    }
}
