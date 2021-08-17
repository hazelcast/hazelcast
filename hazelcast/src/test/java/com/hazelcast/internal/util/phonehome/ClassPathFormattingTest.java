package com.hazelcast.internal.util.phonehome;

import org.junit.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.String.join;
import static org.junit.Assert.assertEquals;

public class ClassPathFormattingTest {

    public static final String PATH_SEPARATOR = System.getProperty("path.separator");

    public static final String FILE_SEPARATOR = System.getProperty("file.separator");

    private static String path(String ...fileNames) {
        return FILE_SEPARATOR + join(FILE_SEPARATOR, fileNames);
    }

    @Test
    public void testJarPathRemovals() {
        String classpath = join(PATH_SEPARATOR,
                path("home", "log4j.jar"),
                path("home", "user", "target"),
                path("home", "user", "targetjar"),
                path("hibernate-validator.jar"),
                path("var", "lib", "jackson-databind.jar")
        );
        String actual = BuildInfoCollector.formatClassPath(classpath);
        assertEquals("log4j.jar:hibernate-validator.jar:jackson-databind.jar", actual);
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
