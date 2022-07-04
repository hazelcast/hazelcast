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

package com.hazelcast.internal.util.phonehome;

import org.junit.Test;

import java.io.File;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.String.join;
import static org.junit.Assert.assertEquals;

public class ClassPathFormattingTest {

    private static String path(String ...fileNames) {
        return File.separator + join(File.separator, fileNames);
    }

    @Test
    public void testJarPathRemovals() {
        String classpath = join(File.pathSeparator,
                path("home", "log4j.jar"),
                path("home", "user", "target"),
                path("home", "user", "targetjar"),
                path("hibernate-validator.jar"),
                path("var", "lib", "jackson-databind.jar")
        );
        String actual = BuildInfoCollector.formatClassPath(classpath);
        assertEquals("log4j.jar,hibernate-validator.jar,jackson-databind.jar", actual);
    }

    @Test
    public void maxSizeLimit() {
        String longClassPath = IntStream.range(0, 30_000)
                .mapToObj(i -> ".jar") // longClassPath.length() will be 120_000
                .collect(Collectors.joining());
        String actual = BuildInfoCollector.formatClassPath(longClassPath);
        assertEquals(100_000, actual.length());
    }

}
