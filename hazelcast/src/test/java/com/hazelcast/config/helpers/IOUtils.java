/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config.helpers;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class IOUtils {
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static File createConfigFile(String filename, String suffix) throws IOException {
        File file = File.createTempFile(filename, suffix);
        file.setWritable(true);
        file.deleteOnExit();
        return file;
    }

    public static void writeStringToStreamAndClose(FileOutputStream os, String string) throws IOException {
        os.write(string.getBytes());
        os.flush();
        os.close();
    }

    public static String createFileWithContent(String filename, String suffix, String content) throws IOException {
        File file = createConfigFile(filename, suffix);
        FileOutputStream os = new FileOutputStream(file);
        writeStringToStreamAndClose(os, content);
        return file.getAbsolutePath();
    }
}
