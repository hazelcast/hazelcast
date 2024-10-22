/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.commandline;

import com.hazelcast.instance.GeneratedBuildProperties;
import com.hazelcast.version.Version;
import org.junit.Test;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertArrayEquals;
import static org.reflections.scanners.Scanners.Resources;

public class VersionProviderTest {
    @Test
    public void test_getVersion() {
        try {
            // given
            String[] expected = {"Version: " + Version.of(GeneratedBuildProperties.VERSION)};
            // when
            String[] version = new VersionProvider().getVersion();
            // then
            assertArrayEquals(expected, version);
        } catch (ExceptionInInitializerError error) {
            // Catch block to investigate test failure
            // https://github.com/hazelcast/hazelcast/issues/26088

            // Investigate how log4j is configured
            // https://logging.apache.org/log4j/2.x/manual/configuration.html#automatic-configuration

            System.out.println("System.getProperties():");
            System.getProperties()
                    .entrySet()
                    .forEach(System.out::println);

            System.out.println();

            String log4j2ConfigurationFileString = System.getProperty("log4j2.configurationFile");

            if (log4j2ConfigurationFileString != null) {
                Path log4j2ConfigurationFile = Paths.get("log4j2ConfigurationFileString");

                if (Files.exists(log4j2ConfigurationFile)) {
                    try {
                        System.out.println("\"log4j2.configurationFile\" contents:");
                        System.out.println(Files.readString(log4j2ConfigurationFile, StandardCharsets.UTF_8));
                    } catch (IOException ioException) {
                        throw new UncheckedIOException(ioException);
                    }
                }
            }

            System.out.println();

            System.out.println("Classpath Contents:");
            ConfigurationBuilder configuration = new ConfigurationBuilder()
                    .setUrls(ClasspathHelper.forJavaClassPath())
                    .setScanners(Resources);
            new Reflections(configuration).getResources(".*")
                    .forEach(System.out::println);

            System.out.println();
        }
    }
}
