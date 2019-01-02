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

import com.google.common.base.Joiner;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.json.PrettyPrint;
import com.hazelcast.internal.json.RandomPrint;
import com.hazelcast.internal.json.WriterConfig;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertNull;

/**
 * This test automatically tests {@link JsonSchemaHelper#findPattern(BufferObjectDataInput, int, JsonSchemaDescription, String[])}
 * and {@link JsonSchemaHelper#findValueWithPattern(BufferObjectDataInput, int, JsonSchemaDescription, List, String[])}
 * methods.
 *
 * It runs the mentioned methods on pre-determined {@code JsonValue}s.
 * The tests use wrong attribute path for extracting values from jsons.
 * Also, correct attribute path wrong expected pattern is tested. The
 * helper should return {@code null} for all cases.
 *
 * The scenarios tested here;
 * <ul>
 *     <li>
 *         path x.y.z when the json has only x.y (y is a terminal value)
 *     </li>
 *     <li>
 *         path a.x.y when the json has only x.y
 *     </li>
 *     <li>
 *         path x.a.z when the json has x.y.z (a != y)
 *     </li>
 * </ul>
 * The above scenarios are tested first as x, y, z, a being attribute
 * names and indexes. Later, the same scenarios are tested with correct
 * attribute names but as x, y, z, a representing an incorrect expected
 * pattern.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class JsonSchemaHelperNullTest extends AbstractJsonSchemaTest {

    private Random random = new Random();

    @Test
    public void testOne() throws IOException {
        testOne(6);
    }

    @Test
    public void testInvalidPaths_MinimalPrint() throws IOException {
        testPaths(WriterConfig.MINIMAL);
    }

    @Test
    public void testInvalidPaths_PrettyPrint() throws IOException {
        testPaths(PrettyPrint.PRETTY_PRINT);
    }

    @Test
    public void testInvalidPaths_RandomPrint() throws IOException {
        testPaths(RandomPrint.RANDOM_PRINT);
    }

    @Override
    protected void validateJsonLiteral(JsonValue expectedValue, String originalString, List<Integer> expectedPattern, JsonSchemaDescription description, BufferObjectDataInput input, String path) throws IOException {
        for (InvalidPathType invalidChoice: InvalidPathType.values()) {
            String[] correctPath = splitPath(path);
            String[] incorrectPath = createWrongPath(path, invalidChoice);

            String wrongPathString = Joiner.on(".").join(incorrectPath);
            List<Integer> pattern = JsonSchemaHelper.findPattern(input, 12, description, incorrectPath);
            assertNull(String.format("Path ( %s ) failed on ( %s )", wrongPathString, originalString), pattern);
            System.out.println("checked path: " + wrongPathString);

            List<Integer> correctPattern = JsonSchemaHelper.findPattern(input, 12, description, correctPath);
            List<Integer> incorrectPattern = createWrongPattern(correctPattern, invalidChoice);
            JsonValue foundValue = JsonSchemaHelper.findValueWithPattern(input, 12, description, incorrectPattern, correctPath);
            assertNull(String.format("Path ( %s ) with pattern ( %s ) failed on ( %s )", path, Arrays.toString(incorrectPattern.toArray()), originalString), foundValue);
            System.out.println(String.format("checked correct path ( %s ) with wrong pattern ( %s )", path, Arrays.toString(incorrectPattern.toArray())));
        }

    }

    private String[] createWrongPath(String attributePath, InvalidPathType invalidChoice) {
        String[] pathParts = super.splitPath(attributePath);
        if (pathParts.length == 0) {
            throw new HazelcastException("not intended");
        }
        if (invalidChoice == InvalidPathType.WRONG_ATTRIBUTE) {
            int wrongAttributeIndex = random.nextInt(pathParts.length);
            if (pathParts[wrongAttributeIndex].endsWith("]")) {
                pathParts[wrongAttributeIndex] = "111]";
            } else {
                pathParts[wrongAttributeIndex] = "invalidAttributeName";
            }
            return pathParts;
        } else if (invalidChoice == InvalidPathType.EXTRA_ATTRIBUTE_BEFORE) {
            String[] wrongPath = new String[pathParts.length + 1];
            wrongPath[0] = "invalidAttribute";
            for (int i = 1; i < wrongPath.length; i++) {
                wrongPath[i] = pathParts[i - 1];
            }
            return wrongPath;
        } else {
            String[] wrongPath = new String[pathParts.length + 1];
            wrongPath[wrongPath.length - 1] = "invalidAttribute";
            for (int i = 0; i < pathParts.length; i++) {
                wrongPath[i] = pathParts[i];
            }
            return wrongPath;
        }
    }

    private List<Integer> createWrongPattern(List<Integer> givenPattern, InvalidPathType invalidChoice) {
        List<Integer> invalidPattern = new ArrayList<Integer>(givenPattern);
        if (givenPattern.size() == 0 || invalidChoice == InvalidPathType.EXTRA_ATTRIBUTE_AFTER) {
            invalidPattern.add(0);
        } else if (invalidChoice == InvalidPathType.EXTRA_ATTRIBUTE_BEFORE) {
            invalidPattern.add(0, 0);
        } else {
            int wrongPatternIndex = random.nextInt(givenPattern.size());
            invalidPattern.set(wrongPatternIndex, givenPattern.get(wrongPatternIndex) + 1);
        }
        return invalidPattern;
    }

    enum InvalidPathType {
        EXTRA_ATTRIBUTE_BEFORE,
        WRONG_ATTRIBUTE,
        EXTRA_ATTRIBUTE_AFTER
    }
}
