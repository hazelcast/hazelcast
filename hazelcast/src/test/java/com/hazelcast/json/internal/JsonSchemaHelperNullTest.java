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

package com.hazelcast.json.internal;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.serialization.impl.NavigableJsonInputAdapter;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.IOException;
import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JsonSchemaHelperNullTest extends AbstractJsonSchemaTest {

    @Parameters(name = "InMemoryFormat: {0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY},
                {InMemoryFormat.OBJECT}
        });
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    private static final String SIMPLE_ARRAY = Json.array(new int[]{1, 2, 3, 4}).toString();
    private static final String SIMPLE_OBJECT = Json.object()
            .add("a", 1)
            .add("b", 2)
            .add("c", 3)
            .toString();
    private static final String NESTED_OBJECT = Json.object()
            .add("a", 1)
            .add("b", Json.object()
                    .add("ba", 21)
                    .add("bb", 22)
                    .add("bc", 23))
            .add("c", Json.array(new int[]{31, 32, 33, 34}))
            .add("d", true)
            .toString();

    // tests on simple array
    @Test
    public void test_givenInvalidPath_shouldCreateNullPattern_pathAttribute() throws IOException {
        test_givenInvalidPattern_createNullPattern(SIMPLE_ARRAY, "abc");
    }

    @Test
    public void test_givenInvalidPath_shouldCreateNullPattern_whenArray_pathIndex() throws IOException {
        test_givenInvalidPattern_createNullPattern(SIMPLE_ARRAY, "[4]");
    }

    @Test
    public void test_givenWrongPattern_returnNull_whenArray_queriedByWrongIndex() throws IOException {
        test_givenWrongPattern_returnNull(SIMPLE_ARRAY, "[1]", "[2]");
    }

    @Test
    public void test_givenWrongPattern_returnNull_whenArray_queriedByAttribute() throws IOException {
        test_givenWrongPattern_returnNull(SIMPLE_ARRAY, "[1]", "someAttribute");
    }

    @Test
    public void test_givenWrongPattern_returnNull_whenArray_queriedByExtraAttribute() throws IOException {
        test_givenWrongPattern_returnNull(SIMPLE_ARRAY, "[1]", "[1].abc");
    }

    @Test
    public void test_givenWrongPattern_returnNull_whenArray_queriedByExtraIndex() throws IOException {
        test_givenWrongPattern_returnNull(SIMPLE_ARRAY, "[1]", "[1][0]");
    }

    // tests on simple object

    @Test
    public void test_givenInvalidPath_shouldCreateNullPattern_whenObject_pathAttribute() throws IOException {
        test_givenInvalidPattern_createNullPattern(SIMPLE_OBJECT, "abc");
    }

    @Test
    public void test_givenInvalidPath_shouldCreateNullPattern_whenObject_pathExtraAttribute() throws IOException {
        test_givenInvalidPattern_createNullPattern(SIMPLE_OBJECT, "b.abc");
    }

    @Test
    public void test_givenInvalidPath_shouldCreateNullPattern_whenObject_pathIndex() throws IOException {
        test_givenInvalidPattern_createNullPattern(SIMPLE_OBJECT, "[0]");
    }

    @Test
    public void test_givenWrongPattern_returnNull_whenObject_queriedByWrongAttribute() throws IOException {
        test_givenWrongPattern_returnNull(SIMPLE_OBJECT, "a", "b");
    }

    @Test
    public void test_givenWrongPattern_returnNull_whenObject_queriedByIndex() throws IOException {
        test_givenWrongPattern_returnNull(SIMPLE_OBJECT, "b", "[1]");
    }

    @Test
    public void test_givenWrongPattern_returnNull_whenObject_queriedByExtraAttribute() throws IOException {
        test_givenWrongPattern_returnNull(SIMPLE_OBJECT, "b", "b.abc");
    }

    @Test
    public void test_givenWrongPattern_returnNull_whenObject_queriedByExtraIndex() throws IOException {
        test_givenWrongPattern_returnNull(SIMPLE_OBJECT, "b", "b[0]");
    }

    // tests on nested object

    @Test
    public void testNestedObject_givenInvalidPath_shouldCreateNullPattern_whenObject_pathExtraAttribute() throws IOException {
        test_givenInvalidPattern_createNullPattern(NESTED_OBJECT, "b.ba.a");
    }

    @Test
    public void testNestedObject_givenInvalidPath_shouldCreateNullPattern_whenObject_pathInvalidAttribute() throws IOException {
        test_givenInvalidPattern_createNullPattern(NESTED_OBJECT, "b.bd");
    }

    @Test
    public void testNestedObject_givenInvalidPath_shouldCreateNullPattern_whenObject_pathIndex() throws IOException {
        test_givenInvalidPattern_createNullPattern(NESTED_OBJECT, "b[0]");
    }

    @Test
    public void testNestedObject_givenInvalidPath_shouldCreateNullPattern_whenArray_pathExtraIndex() throws IOException {
        test_givenInvalidPattern_createNullPattern(NESTED_OBJECT, "c[1][1]");
    }

    @Test
    public void testNestedObject_givenInvalidPath_shouldCreateNullPattern_whenArray_pathInvalidIndex() throws IOException {
        test_givenInvalidPattern_createNullPattern(NESTED_OBJECT, "c[5]");
    }

    @Test
    public void testNestedObject_givenInvalidPath_shouldCreateNullPattern_whenArray_pathExtraAttribute() throws IOException {
        test_givenInvalidPattern_createNullPattern(NESTED_OBJECT, "c[0].abc");
    }

    @Test
    public void test_givenWrongPattern_returnNull_whenNestedObject_queriedByWrongAttribute() throws IOException {
        test_givenWrongPattern_returnNull(NESTED_OBJECT, "b.bb", "b.bc");
    }

    @Test
    public void test_givenWrongPattern_returnNull_whenNestedObject_queriedByMissingAttribute() throws IOException {
        test_givenWrongPattern_returnNull(NESTED_OBJECT, "b.bb", "b");
    }

    @Test
    public void test_givenWrongPattern_returnNull_whenNestedObject_queriedByIndex() throws IOException {
        test_givenWrongPattern_returnNull(NESTED_OBJECT, "b.bb", "b[1]");
    }

    @Test
    public void test_givenWrongPattern_returnNull_whenNestedObject_queriedByExtraAttribute() throws IOException {
        test_givenWrongPattern_returnNull(NESTED_OBJECT, "b.bb", "b.bb.abc");
    }

    @Test
    public void test_givenWrongPattern_returnNull_whenNestedObject_queriedByExtraIndex() throws IOException {
        test_givenWrongPattern_returnNull(NESTED_OBJECT, "b.bb", "b.bb[0]");
    }

    @Test
    public void test_givenWrongPattern_returnNull_whenNestedArray_queriedByAttribute() throws IOException {
        test_givenWrongPattern_returnNull(NESTED_OBJECT, "c[1]", "c.cb");
    }

    @Test
    public void test_givenWrongPattern_returnNull_whenNestedArray_queriedByMissingIndex() throws IOException {
        test_givenWrongPattern_returnNull(NESTED_OBJECT, "c[1]", "c");
    }

    @Test
    public void test_givenWrongPattern_returnNull_whenNestedArray_queriedByWrongIndex() throws IOException {
        test_givenWrongPattern_returnNull(NESTED_OBJECT, "c[1]", "c[0]");
    }

    @Test
    public void test_givenWrongPattern_returnNull_whenNestedArray_queriedByExtraAttribute() throws IOException {
        test_givenWrongPattern_returnNull(NESTED_OBJECT, "c[1]", "c[1].abc");
    }

    @Test
    public void test_givenWrongPattern_returnNull_whenNestedArray_queriedByExtraIndex() throws IOException {
        test_givenWrongPattern_returnNull(NESTED_OBJECT, "c[1]", "c[1][0]");
    }

    @Override
    protected InMemoryFormat getInMemoryFormay() {
        return inMemoryFormat;
    }

    private void test_givenInvalidPattern_createNullPattern(String jsonString, String path) throws IOException {
        NavigableJsonInputAdapter inputAdapter = toAdapter(new HazelcastJsonValue(jsonString));
        JsonSchemaNode schemaNode = JsonSchemaHelper.createSchema(createParserFromInput(inputAdapter));
        JsonPattern pattern = JsonSchemaHelper.createPattern(inputAdapter, schemaNode, splitPath(path));
        assertNull(pattern);
    }

    private void test_givenWrongPattern_returnNull(String jsonString, String patternPath, String queryPath) throws IOException {
        NavigableJsonInputAdapter input = toAdapter(new HazelcastJsonValue(jsonString));
        JsonSchemaNode description = JsonSchemaHelper.createSchema(createParserFromInput(input));
        JsonPattern pattern = JsonSchemaHelper.createPattern(input, description, splitPath(patternPath));

        JsonValue found = JsonSchemaHelper.findValueWithPattern(input, description, pattern, splitPath(queryPath));
        assertNull(found);
    }
}
