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

package com.hazelcast.test;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;

/**
 * Utility methods to facilitate referencing files from {@code src/test/class} which are not part of the classpath at
 * test execution time.
 */
public class UserCodeUtil {

    public static final String CLASS_DIRECTORY = "src/test/class";
    public static final File CLASS_DIRECTORY_FILE = new File(CLASS_DIRECTORY);

    private UserCodeUtil() {
    }

    /** @return a File for the given path, relative to src/test/class */
    public static File fileRelativeToBinariesFolder(String path) {
        return new File(CLASS_DIRECTORY_FILE, path);
    }

    /** @see #fileRelativeToBinariesFolder(String) */
    public static String pathRelativeToBinariesFolder(String path) {
        return fileRelativeToBinariesFolder(path).toString();
    }

    public static URL urlFromFile(File f) {
        try {
            return f.toURI().toURL();
        } catch (MalformedURLException e) {
            throw sneakyThrow(e);
        }
    }
}
