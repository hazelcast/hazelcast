/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.fasterxml.jackson.core.JsonParser;
import com.hazelcast.internal.json.Json;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category({ParallelTest.class, QuickTest.class})
@RunWith(HazelcastParallelClassRunner.class)
public class JsonSchemaCreatorTest extends AbstractJsonSchemaTest {

    @Test
    public void testOneFirstLevelAttribute() throws IOException {
        String jsonString = Json.object().add("name", "aName").toString();
        JsonParser parser = createParserFromString(jsonString);
        JsonSchemaDescription description = JsonSchemaHelper.createDescription(parser);

        validate(description, "0", 1, 8);
    }

    @Test
    public void testOneFirstLevelAttribute_withTwoByteCharacterInName() throws IOException {
        String jsonString = Json.object().add("this-name-includes-two-byte-utf8-character-£",
                "so the value should start at next byte location").toString();
        JsonParser parser = createParserFromString(jsonString);
        JsonSchemaDescription description = JsonSchemaHelper.createDescription(parser);

        validate(description, "0", 1, 49);
    }

    @Test
    public void testTwoFirstLevelAttributes() throws IOException {
        String jsonString = Json.object().add("name", "aName").add("age", 4).toString();
        JsonParser parser = createParserFromString(jsonString);
        JsonSchemaStructDescription description = (JsonSchemaStructDescription) JsonSchemaHelper.createDescription(parser);

        validate(description, "0", 1, 8);
        validate(description, "1", 16, 22);
    }

    @Test
    public void testThreeFirstLevelAttributes() throws IOException {
        String jsonString = Json.object()
                .add("name", "aName")
                .add("age", 4)
                .add("location", "ankara")
                .toString();

        JsonParser parser = createParserFromString(jsonString);
        JsonSchemaStructDescription description = (JsonSchemaStructDescription) JsonSchemaHelper.createDescription(parser);

        validate(description, "0", 1, 8);
        validate(description, "1", 16, 22);
        validate(description, "2", 24, 35);
    }

    @Test
    public void testOneFirstLevelTwoInnerAttributes() throws IOException {
        String jsonString = Json.object()
                .add("name", Json.object()
                        .add("firstName", "fName")
                        .add("surname", "sname")
                )
                .toString();

        JsonParser parser = createParserFromString(jsonString);
        JsonSchemaStructDescription description = (JsonSchemaStructDescription) JsonSchemaHelper.createDescription(parser);

        validate(description, "0.0", 9, 21);
        validate(description, "0.1", 29, 39);
    }

    @Test
    public void testTwoFirstLevelOneInnerAttributesEach() throws IOException {
        String jsonString = Json.object()
                .add("name", Json.object()
                        .add("firstName", "fName"))
                .add("address", Json.object()
                        .add("addressId", 4))
                .toString();

        JsonParser parser = createParserFromString(jsonString);
        JsonSchemaStructDescription description = (JsonSchemaStructDescription) JsonSchemaHelper.createDescription(parser);

        validate(description, "0.0", 9, 21);
        validate(description, "1.0", 41, 53);
    }

    @Test
    public void testFourNestedLevelsEachHavingAValue() throws IOException {
        String jsonString = Json.object()
                .add("firstObject", Json.object()
                        .add("secondObject", Json.object()
                                .add("thirdLevelTerminalString", "terminalvalue")
                                .add("thirdObject", Json.object()
                                        .add("fourthTerminalValue", true)))
                        .add("secondLevelTerminalNumber", 43534324))
                .add("firstLevelTerminalNumber", 53).toString();

        JsonParser parser = createParserFromString(jsonString);
        JsonSchemaStructDescription description = (JsonSchemaStructDescription) JsonSchemaHelper.createDescription(parser);

        validate(description, "1", 157, 184);
        validate(description, "0.1", 119, 147);
        validate(description, "0.0.0", 32, 59);
        validate(description, "0.0.1.0", 90, 112);
    }


    @Test
    public void testFourNestedLevels() throws IOException {
        String jsonString = Json.object()
                .add("firstObject", Json.object()
                        .add("secondObject", Json.object()
                                .add("thirdObject", Json.object()
                                        .add("fourthObject", true)))).toString();

        JsonParser parser = createParserFromString(jsonString);
        JsonSchemaDescription description = JsonSchemaHelper.createDescription(parser);

        validate(description, "0.0.0.0", 47, 62);
    }

    @Test
    public void testFirstLevelValue() throws IOException {
        String jsonString = Json.value("name").toString();

        JsonParser parser = createParserFromString(jsonString);
        JsonSchemaDescription description = JsonSchemaHelper.createDescription(parser);

        validate(description, "this", -1, 0);
    }

    @Test
    public void testSimpleArray() throws IOException {
        String jsonString = Json.array(1, 2, 3).toString();

        printWithGuides(jsonString);
        JsonParser parser = createParserFromString(jsonString);
        JsonSchemaDescription description = JsonSchemaHelper.createDescription(parser);

        validate(description, "[0]", -1, 1);
        validate(description, "[1]", -1, 3);
        validate(description, "[2]", -1, 5);
    }

    protected void validate(JsonSchemaDescription root, String attributePath, int expectedNameLoc, int expectedValueLoc) {
        assertNotNull(root);
        if (attributePath.equals("this")) {
            JsonSchemaTerminalDescription leaf = (JsonSchemaTerminalDescription) root;
            assertEquals(expectedValueLoc, leaf.getValueStartLocation());
        } else {
            JsonSchemaDescription schemaDescription = (JsonSchemaStructDescription) root;
            String[] path = attributePath.split("\\.");
            JsonSchemaNameValue nameValue;
            int nameLoc = 1000;
            JsonSchemaDescription object;
            for (String p : path) {
                nameValue = ((JsonSchemaStructDescription) schemaDescription).getChild(c(p));
                nameLoc = nameValue.getNameStart();
                schemaDescription = nameValue.getValue();
            }
            assertEquals(String.format("\nExpected name location: %d\nActual name location: %d\n", expectedNameLoc, nameLoc), expectedNameLoc, nameLoc);
            int actualValueLoc = ((JsonSchemaTerminalDescription) schemaDescription).getValueStartLocation();
            assertEquals(String.format("\nExpected value location: %d\nActual value location: %d\n", expectedValueLoc, actualValueLoc), expectedValueLoc, actualValueLoc);
        }
    }

    private int c(String num) {
        if (num.startsWith("[")) {
            return Integer.parseInt(num.substring(1, num.length() - 1));
        }
        return Integer.parseInt(num);
    }
}
