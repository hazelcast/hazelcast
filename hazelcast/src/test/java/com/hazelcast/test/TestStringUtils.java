/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import static com.hazelcast.internal.nio.IOUtil.closeResource;

public final class TestStringUtils {

    private static final int READ_BUFFER_SIZE = 8192;

    private TestStringUtils() {
    }

    public static String fileAsText(File file) {
        FileInputStream stream = null;
        InputStreamReader streamReader = null;
        BufferedReader reader = null;
        try {
            stream = new FileInputStream(file);
            streamReader = new InputStreamReader(stream);
            reader = new BufferedReader(streamReader);

            StringBuilder builder = new StringBuilder();
            char[] buffer = new char[READ_BUFFER_SIZE];
            int read;
            while ((read = reader.read(buffer, 0, buffer.length)) > 0) {
                builder.append(buffer, 0, read);
            }
            return builder.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            closeResource(reader);
            closeResource(streamReader);
            closeResource(stream);
        }
    }

    public static String repeat(String string, int times) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < times; ++i) {
            result.append(string);
        }
        return result.toString();
    }

}
