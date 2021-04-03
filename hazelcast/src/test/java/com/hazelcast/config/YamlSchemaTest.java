package com.hazelcast.config;

import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class YamlSchemaTest {

    public static final Schema SCHEMA = SchemaLoader.builder()
            .schemaJson(readJSONObject("/hazelcast-config-5.0.json"))
            .build()
            .load().build();
    public static final String TESTCASE_ROOT_DIR = "com/hazelcast/config/yaml/testcases/";

    private static final JSONObject readJSONObject(String absPath) {
        return new JSONObject(new JSONTokener(YamlSchemaTest.class.getResourceAsStream(absPath)));
    }

    @Parameterized.Parameters
    public static List<Object[]> buildTestcases() {
        ConfigurationBuilder configuration = new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forJavaClassPath())
                .setScanners(new ResourcesScanner());
        Reflections reflections = new Reflections(configuration);
        return reflections.getResources(Pattern.compile(".*\\.json")).stream()
                .filter(e -> e.startsWith(TESTCASE_ROOT_DIR))
                .map(path -> buildArgs(path))
                .collect(toList());
    }

    private static Object[] buildArgs(String path) {
        JSONObject testcase = readJSONObject("/" + path);
        Object error = testcase.get("error");
        if (error == JSONObject.NULL) {
            error = null;
        }
        String testName = path.substring(TESTCASE_ROOT_DIR.length(), path.length() - 5);
        return new Object[]{testName, testcase.getJSONObject("instance"), error};
    }

    private void sortCauses(JSONObject exc) {
        JSONArray causes = exc.optJSONArray("causingExceptions");
        if (causes != null) {
            List<JSONObject> causesList = new ArrayList<>(causes.length());
            for (int i = 0; i < causes.length(); ++i) {
                JSONObject item = causes.getJSONObject(i);
                sortCauses(item);
                causesList.add(item);
            }
            causesList.sort(Comparator.comparing(Object::toString));
            exc.put("causingExceptions", new JSONArray(causesList));
        }
    }
    
    private final String testName; 
    private final JSONObject input;
    private final JSONObject expectedValidationError;

    public YamlSchemaTest(String testName, JSONObject input, JSONObject expectedValidationError) {
        this.testName = testName;
        this.input = input;
        this.expectedValidationError = expectedValidationError;
    }

    @Test
    public void runTest() {
        try {
            SCHEMA.validate(input);
            if (expectedValidationError != null) {
                fail("did not throw exception");
            }
        } catch (ValidationException e) {
            if (expectedValidationError == null) {
                System.err.println(e.toJSON().toString(2));
                fail("unexpected exception: " + e.getMessage());
            } else {
                sortCauses(expectedValidationError);
                JSONObject actualJson = e.toJSON();
                sortCauses(actualJson);
                assertEquals(expectedValidationError.toString(2), actualJson.toString(2));
            }
        }
    }
    
}
