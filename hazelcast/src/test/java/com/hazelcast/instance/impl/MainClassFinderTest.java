package com.hazelcast.instance.impl;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.fail;

public class MainClassFinderTest {

    @Test
    public void testFindMainClassInManifest() throws Exception {
        String jarPath = getPathOfManifestJar();

        MainClassFinder mainClassFinder = new MainClassFinder();
        mainClassFinder.findMainClass(null, jarPath);
        String mainClassName = mainClassFinder.getMainClassName();
        Assertions.assertThat(mainClassName)
                .isEqualTo("org.example.Main");
    }

    @Test
    public void testNoMainClassInManifest() throws Exception {
        String jarPath = getPathOfNoManifestJar();

        MainClassFinder mainClassFinder = new MainClassFinder();
        mainClassFinder.findMainClass(null, jarPath);
        String mainClassName = mainClassFinder.getMainClassName();
        Assertions.assertThat(mainClassName)
                .isNull();

        String errorMessage = mainClassFinder.getErrorMessage();
        Assertions.assertThat(errorMessage)
                .containsIgnoringCase("No Main-Class found in the manifest of ");

    }

    private String getPathOfManifestJar() {
        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource("simplejob-1.0.0.jar");
        Path result = null;
        try {
            assert resource != null;
            result = Paths.get(resource.toURI());
        } catch (Exception exception) {
            fail("Unable to get jar path from :" + resource);
        }
        return result.toString();
    }

    private String getPathOfNoManifestJar() {
        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource("nomanifestsimplejob-1.0.0 .jar");
        Path result = null;
        try {
            assert resource != null;
            result = Paths.get(resource.toURI());
        } catch (Exception exception) {
            fail("Unable to get jar path from :" + resource);
        }
        return result.toString();
    }
}
