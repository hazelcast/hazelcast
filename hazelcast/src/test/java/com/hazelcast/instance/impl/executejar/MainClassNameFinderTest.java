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

package com.hazelcast.instance.impl.executejar;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.fail;

public class MainClassNameFinderTest {

    @Test
    public void testFindMainClassInManifest() throws Exception {
        String jarPath = getPathOfJar("simplejob-1.0.0.jar");

        MainClassNameFinder mainClassNameFinder = new MainClassNameFinder();
        mainClassNameFinder.findMainClass(jarPath, null);
        String mainClassName = mainClassNameFinder.getMainClassName();
        Assertions.assertThat(mainClassName)
                .isEqualTo("org.example.Main");
    }

    @Test
    public void testNoMainClassInManifest() throws Exception {
        String jarPath = getPathOfJar("nomanifestsimplejob-1.0.0.jar");

        MainClassNameFinder mainClassNameFinder = new MainClassNameFinder();
        mainClassNameFinder.findMainClass(jarPath, null);
        String mainClassName = mainClassNameFinder.getMainClassName();
        Assertions.assertThat(mainClassName)
                .isNull();

        String errorMessage = mainClassNameFinder.getErrorMessage();
        Assertions.assertThat(errorMessage)
                .containsIgnoringCase("No Main-Class found in the manifest of ");
    }

    private String getPathOfJar(String jarName) {
        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource(jarName);
        Path result = null;
        try {
            assert resource != null;
            result = Paths.get(resource.toURI());
        } catch (Exception exception) {
            fail("Unable to get jar path from :" + jarName);
        }
        return result.toString();
    }
}
