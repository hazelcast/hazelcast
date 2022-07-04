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

package com.hazelcast.config;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaClient;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.Test;
import org.junit.experimental.categories.Category;
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
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public abstract class AbstractYamlSchemaTest {

    private static final ILogger LOGGER = Logger.getLogger(AbstractYamlSchemaTest.class);

    public static final Schema SCHEMA = SchemaLoader.builder()
            .schemaJson(readJSONObject("/hazelcast-config-5.2.json"))
            .draftV6Support()
            .schemaClient(SchemaClient.classPathAwareClient())
            .build()
            .load().build();

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
            SCHEMA.validate(input);
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

}
