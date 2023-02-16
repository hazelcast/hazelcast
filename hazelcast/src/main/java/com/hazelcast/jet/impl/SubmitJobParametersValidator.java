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
import java.util.Objects;

public class SubmitJobParametersValidator {

    // Validate the parameters used by the client
    void validateParameterObject(SubmitJobParameters parameterObject) throws IOException {
        // Check that parameter is not null, because it is used to access the file
        if (Objects.isNull(parameterObject.getJarPath())) {
            throw new JetException("jarPath can not be null");
        }

        // Check that parameter is not null, because it is used by the JetUploadJobMetaDataCodec
        if (Objects.isNull(parameterObject.getJobParameters())) {
            throw new JetException("jobParameters can not be null");
        }
        long jarSize = Files.size(parameterObject.getJarPath());
        if (jarSize == 0) {
            throw new JetException("Jar size can not be 0");
        }

    }
}
