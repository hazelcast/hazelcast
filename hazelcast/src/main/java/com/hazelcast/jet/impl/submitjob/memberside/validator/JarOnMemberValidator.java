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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/**
 * Java client has validation but non-java clients may not perform validation.
 * This class performs the validation on the member side
 */
public final class JarOnMemberValidator {

    private JarOnMemberValidator() {
    }

    public static void validate(JobMetaDataParameterObject parameterObject) {
        Path jarPath = parameterObject.getJarPath();
        validateJarPathNotNull(jarPath);
        validateFileSizeIsNotZero(jarPath);
        validateFileExtension(jarPath);
        validateJobParameters(parameterObject.getJobParameters());
    }

    static void validateJarPathNotNull(Path jarPath) {
        // Check that parameter is not null, because it is used to access the file
        if (Objects.isNull(jarPath)) {
            throw new JetException("File path can not be null");
        }
    }

    static void validateFileExtension(Path jarPath) {
        String fileName = jarPath.getFileName().toString();
        if (!fileName.endsWith(".jar")) {
            throw new JetException("File name extension should be .jar");
        }
    }

    static void validateFileSizeIsNotZero(Path jarPath) {
        // Check that the file exists and its size is not 0
        try {
            long jarSize = Files.size(jarPath);
            if (jarSize == 0) {
                throw new JetException("File size can not be 0");
            }
        } catch (IOException exception) {
            throw new JetException("File is not accessible ", exception);
        }
    }

    static void validateJobParameters(List<String> jobParameters) {
        // Check that parameter is not null
        if (Objects.isNull(jobParameters)) {
            throw new JetException("jobParameters can not be null");
        }
    }
}
