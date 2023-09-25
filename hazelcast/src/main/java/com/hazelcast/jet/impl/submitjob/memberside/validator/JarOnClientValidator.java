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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;

/**
 * Java client has validation but non-java clients may not perform validation.
 * This class performs the validation on the member side
 */
public final class JarOnClientValidator {

    private JarOnClientValidator() {
    }

    public static void validate(JobMetaDataParameterObject parameterObject) {
        validateUploadDirectoryPath(parameterObject.getUploadDirectoryPath());
        validateJobParameters(parameterObject.getJobParameters());
    }

    static void validateUploadDirectoryPath(String  uploadDirectoryPath) {
        if (uploadDirectoryPath != null) {
            Path path = Paths.get(uploadDirectoryPath);
            if (!Files.exists(path)) {
                String errorMessage = String.format("The upload directory path does not exist: %s", path);
                throw new JetException(errorMessage);
            }
        }
    }
    static void validateJobParameters(List<String> jobParameters) {
        // Check that parameter is not null
        if (Objects.isNull(jobParameters)) {
            throw new JetException("jobParameters can not be null");
        }
    }
}
