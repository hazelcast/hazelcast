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
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.json.PrettyPrint;
import com.hazelcast.internal.json.RandomPrint;
import com.hazelcast.internal.json.WriterConfig;
import com.hazelcast.internal.serialization.impl.NavigableJsonInputAdapter;
import com.hazelcast.query.impl.getters.JsonPathCursor;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * This test automatically tests
 * {@link JsonSchemaHelper#createPattern(NavigableJsonInputAdapter, JsonSchemaNode, JsonPathCursor)}
 * and
 * {@link JsonSchemaHelper#findValueWithPattern(NavigableJsonInputAdapter, JsonSchemaNode, JsonPattern, JsonPathCursor)}
 * methods.
 *
 * It runs the mentioned methods on pre-determined {@code JsonValue}s.
 * The tests use all valid attribute paths to extract {@code JsonValue}s
 * and compare extracted values with the ones that are available from
 * JsonValue tree.
 *
 * This suite include simple test cases along with automated test cases.
 * These are just there to demonstrate what kind of testing is done and
 * additional peace of mind. They are already covered by the automated
 * tests.
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JsonSchemaHelperTest extends AbstractJsonSchemaTest {

    @Parameters(name = "InMemoryFormat: {0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY},
                {InMemoryFormat.OBJECT}
        });
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Test
    public void testAllValidPaths_MinimalPrint() throws IOException {
        testPaths(WriterConfig.MINIMAL);
    }

    @Test
    public void testAllValidPaths_PrettyPrint() throws IOException {
        testPaths(PrettyPrint.PRETTY_PRINT);
    }

    @Test
    public void testAllValidPaths_RandomPrint() throws IOException {
        testPaths(RandomPrint.RANDOM_PRINT);
    }

    @Test
    public void testQuerySimpleNestedQuery() throws IOException {
        JsonObject object = Json.object()
                .add("inner", Json.object()
                        .add("a", 3)
                        .add("b", 5));

        NavigableJsonInputAdapter input = toAdapter(new HazelcastJsonValue(object.toString()));
        JsonSchemaNode description = JsonSchemaHelper.createSchema(createParserFromInput(input));
        JsonPattern pattern = JsonSchemaHelper.createPattern(input, description, splitPath("inner.b"));
        assertEquals(new JsonPattern(asList(0, 1)), pattern);
    }

    @Test
    public void testEmptyStringReturnsNullSchema() throws IOException {
        NavigableJsonInputAdapter input = toAdapter(new HazelcastJsonValue(""));
        JsonSchemaNode description = JsonSchemaHelper.createSchema(createParserFromInput(input));
        assertNull(description);
    }

    @Test
    public void testOneLevelObject() throws IOException {
        JsonObject object = Json.object()
                .add("a", true)
                .add("b", false)
                .add("c", Json.NULL)
                .add("d", 4)
                .add("e", "asd");

        NavigableJsonInputAdapter input = toAdapter(new HazelcastJsonValue(object.toString()));
        JsonSchemaNode description = JsonSchemaHelper.createSchema(createParserFromInput(input));
        JsonPattern pattern = JsonSchemaHelper.createPattern(input, description, splitPath("b"));
        assertEquals(new JsonPattern(asList(1)), pattern);

        JsonValue found = JsonSchemaHelper.findValueWithPattern(input, description, pattern, splitPath("b"));
        assertEquals(Json.FALSE, found);
    }

    @Test
    public void testQueryToNonTerminalValueCreatesPattern() throws IOException {
        JsonObject object = Json.object()
                .add("a", Json.object()
                        .add("x", 1)
                        .add("y", 2))
                .add("b", false);

        NavigableJsonInputAdapter input = toAdapter(new HazelcastJsonValue(object.toString()));
        JsonSchemaNode description = JsonSchemaHelper.createSchema(createParserFromInput(input));
        JsonPattern pattern = JsonSchemaHelper.createPattern(input, description, splitPath("a"));
        assertEquals(new JsonPattern(asList(0)), pattern);
    }

    @Override
    protected InMemoryFormat getInMemoryFormay() {
        return inMemoryFormat;
    }

}
