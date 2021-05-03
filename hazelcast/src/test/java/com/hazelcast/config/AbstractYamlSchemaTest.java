package com.hazelcast.config;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.Test;
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

public abstract class AbstractYamlSchemaTest {

    private static final ILogger LOGGER = Logger.getLogger(AbstractYamlSchemaTest.class);
    
    static JSONObject readJSONObject(String absPath) {
        return new JSONObject(new JSONTokener(MemberYamlSchemaTest.class.getResourceAsStream(absPath)));
    }

    protected static List<Object[]> buildTestcases(String rootDir) {
        ConfigurationBuilder configuration = new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forJavaClassPath())
                .setScanners(new ResourcesScanner());
        Reflections reflections = new Reflections(configuration);
        return reflections.getResources(Pattern.compile(".*\\.json")).stream()
                .filter(e -> e.startsWith(rootDir))
                .map(path -> buildArgs(rootDir, path))
                .collect(toList());
    }

    private static Object[] buildArgs(String rootDir, String path) {
        JSONObject testcase = readJSONObject("/" + path);
        Object error = testcase.get("error");
        if (error == JSONObject.NULL) {
            error = null;
        }
        String testName = path.substring(rootDir.length(), path.length() - 5);
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

    @Parameterized.Parameter(0)
    public String testName;

    @Parameterized.Parameter(1)
    public JSONObject input;

    @Parameterized.Parameter(2)
    public JSONObject expectedValidationError;

    @Test
    public void runTest() {
        try {
            Schema schema = getSchema();
            schema.validate(input);
            if (expectedValidationError != null) {
                fail("did not throw exception");
            }
        } catch (ValidationException e) {
            if (expectedValidationError == null) {
                LOGGER.severe(e.toJSON().toString(2));
                fail("unexpected exception: " + e.getMessage());
            } else {
                sortCauses(expectedValidationError);
                JSONObject actualJson = e.toJSON();
                sortCauses(actualJson);
                assertEquals(expectedValidationError.toString(2), actualJson.toString(2));
            }
        }
    }

    protected abstract Schema getSchema();

}
