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

package com.hazelcast.jet.impl.submitjob.clientside.upload;

import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class JobUploadCallTest {

    @Test
    public void testFindFileNameWithoutExtension() {
        JobUploadCall jobUploadCall = new JobUploadCall();

        String expectedFileName = "foo";
        Path jarPath = Paths.get("/mnt/foo.jar");
        String fileNameWithoutExtension = jobUploadCall.findFileNameWithoutExtension(jarPath);
        assertEquals(expectedFileName, fileNameWithoutExtension);

        jarPath = Paths.get("foo.jar");
        fileNameWithoutExtension = jobUploadCall.findFileNameWithoutExtension(jarPath);
        assertEquals(expectedFileName, fileNameWithoutExtension);

        jarPath = Paths.get("foo");
        fileNameWithoutExtension = jobUploadCall.findFileNameWithoutExtension(jarPath);
        assertEquals(expectedFileName, fileNameWithoutExtension);
    }
}

