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

package com.hazelcast.internal.serialization.impl.compact.reader;

import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.GenericRecordQueryReader;
import com.hazelcast.internal.serialization.impl.compact.CompactTestUtil;
import com.hazelcast.internal.serialization.impl.compact.SchemaService;
import com.hazelcast.query.impl.getters.MultiResult;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.internal.serialization.impl.compact.reader.CompactValueReaderTestStructure.GroupObject;
import static com.hazelcast.internal.serialization.impl.compact.reader.CompactValueReaderTestStructure.NestedGroupObject;
import static com.hazelcast.internal.serialization.impl.compact.reader.CompactValueReaderTestStructure.PrimitiveFields;
import static com.hazelcast.internal.serialization.impl.compact.reader.CompactValueReaderTestStructure.PrimitiveFields.getPrimitiveArrays;
import static com.hazelcast.internal.serialization.impl.compact.reader.CompactValueReaderTestStructure.PrimitiveFields.getPrimitives;
import static com.hazelcast.internal.serialization.impl.compact.reader.CompactValueReaderTestStructure.PrimitiveObject;
import static com.hazelcast.internal.serialization.impl.compact.reader.CompactValueReaderTestStructure.PrimitiveObject.Init.FULL;
import static com.hazelcast.internal.serialization.impl.compact.reader.CompactValueReaderTestStructure.PrimitiveObject.Init.NONE;
import static com.hazelcast.internal.serialization.impl.compact.reader.CompactValueReaderTestStructure.PrimitiveObject.Init.NULL;
import static com.hazelcast.internal.serialization.impl.compact.reader.CompactValueReaderTestStructure.group;
import static com.hazelcast.internal.serialization.impl.compact.reader.CompactValueReaderTestStructure.nested;
import static com.hazelcast.internal.serialization.impl.compact.reader.CompactValueReaderTestStructure.prim;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertThat;

/**
 * Tests that verifies the behavior of the DefaultObjectReader.
 * All tests cases are generated, since there's a lot of possible cases due to the long lists of read* method on the reader.
 * <p>
 * The test is parametrised with 4 parameters
 * Each test execution runs one read operation on the reader.
 * <p>
 * The rationale behind these tests is to cover all possible combinations of reads using nested paths and quantifiers
 * (number or any) with all possible object types. It's impossible to do it manually, since there's 20 supported
 * types and a read method for each one of them.
 * <p>
 * Each test case is documented, plus each test outputs it's scenario in a readable way, so you it's easy to follow
 * the test case while you run it. Also each test case shows in which method it is generated.
 * <p>
 * IF YOU SEE A FAILURE HERE:
 * - check the test output - analyse the test scenario
 * - check in which method the scenario is generated - narrow down the scope of the tests run
 */
@RunWith(HazelcastParametrizedRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class CompactStreamSerializerValueReaderSpecTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    // input object
    private Object inputObject;
    // object or exception
    private Object expectedResult;
    // e.g. body.brain.iq
    private String pathToRead;
    // parent method of this test to identify it in case of failures
    private String parent;

    @Parameters(name = "{index}: {0}, read{2}, {3}")
    public static Collection<Object[]> parametrisationData() {
        List<Object[]> result = new ArrayList<>();

        directPrimitiveScenarios(result);
        fromObjectToPrimitiveScenarios(result);
        fromObjectArrayToPrimitiveScenarios(result);
        fromObjectToObjectToPrimitiveScenarios(result);
        fromObjectToObjectArrayToPrimitiveScenarios(result);
        fromObjectArrayToObjectArrayToPrimitiveArrayAnyScenarios(result);
        fromObjectArrayAnyToObjectArrayAnyToPrimitiveScenarios(result);
        fromObjectArrayToObjectArrayToPrimitiveScenarios(result);
        fromObjectArrayToObjectArrayAnyScenarios(result);

        return result;
    }

    public CompactStreamSerializerValueReaderSpecTest(Object inputObject, Object expectedResult, String pathToRead, String parent) {
        this.inputObject = inputObject;
        this.expectedResult = expectedResult;
        this.pathToRead = pathToRead;
        this.parent = parent;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void executeTestScenario() throws Exception {
        // handle result
        Object resultToMatch = expectedResult;
        if (expectedResult instanceof Class) {
            // expected exception case
            expected.expect(isA((Class) expectedResult));
        } else if (expectedResult instanceof List) {
            // just convenience -> if result is a list if will be compared to an array, so it has to be converted
            resultToMatch = ((List) resultToMatch).toArray();
        }

        // print test scenario for debug purposes
        // it makes debugging easier since all scenarios are generated
        printlnScenarioDescription(resultToMatch);

        SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.setCompactSerializationConfig(new CompactSerializationConfig().setEnabled(true));
        InternalSerializationService ss = new DefaultSerializationServiceBuilder()
                .setConfig(serializationConfig)
                .setSchemaService(schemaService).build();

        Data data = ss.toData(inputObject);
        GenericRecordQueryReader reader = new GenericRecordQueryReader(ss.readAsInternalGenericRecord(data));

        Object result = reader.read(pathToRead);
        if (result instanceof MultiResult) {
            MultiResult multiResult = (MultiResult) result;
            if (multiResult.getResults().size() == 1
                    && multiResult.getResults().get(0) == null && multiResult.isNullEmptyTarget()) {
                // explode null in case of a single multi-result target result
                result = null;
            } else {
                // in case of multi result while invoking generic "read" method deal with the multi results
                result = ((MultiResult) result).getResults().toArray();
            }
        }
        assertThat(result, equalTo(resultToMatch));

    }

    private void printlnScenarioDescription(Object resultToMatch) {
        String desc = "Running test case:\n";
        desc += "parent:\t" + parent + "\n";
        desc += "path:\t" + pathToRead + "\n";
        desc += "result:\t" + resultToMatch + "\n";
        desc += "input:\t" + inputObject + "\n";
        System.out.println(desc);
    }

    /**
     * Expands test cases for primitive non-array data types.
     * Word "primitive_" from the pathToExplode is replaced by each primitive type and the scenario is expanded to:
     * <ul>
     * <li>scenario(input, result.byte_, adjustedPath + "byte_"),</li>
     * <li>scenario(input, result.short_, adjustedPath + "short_"),</li>
     * <li>scenario(input, result.int_, adjustedPath + "int_"),</li>
     * <li>scenario(input, result.long_, adjustedPath + "long_"),</li>
     * <li>scenario(input, result.float_, adjustedPath + "float_"),</li>
     * <li>scenario(input, result.double_, adjustedPath + "double_"),</li>
     * <li>scenario(input, result.boolean_, adjustedPath + "boolean_"),</li>
     * <li>scenario(input, result.string_, adjustedPath + "string_"),</li>
     * </ul>
     */
    private static Collection<Object[]> expandPrimitiveScenario(Object input, Object result, String pathToExplode,
                                                                String parent) {
        List<Object[]> scenarios = new ArrayList<>();
        Object adjustedResult;
        for (PrimitiveFields primitiveFields : getPrimitives()) {
            if (result instanceof PrimitiveObject) {
                adjustedResult = ((PrimitiveObject) result).getPrimitive(primitiveFields);
            } else {
                adjustedResult = result;
            }
            // generic method case
            scenarios.add(scenario(input, adjustedResult, pathToExplode.replace("primitive_", primitiveFields.field), parent));
        }
        return scenarios;
    }


    /**
     * Expands test cases for primitive array data types.
     * Word "primitiveArray" is replaced by each primitive array type and the scenario is expanded to for each type:
     * <p>
     * group A:
     * <ul>
     * <li>scenario(prim(FULL), prim(FULL).bytes, ByteArray, "bytes"),</li>
     * <li>scenario(prim(NONE), prim(NONE).bytes, ByteArray, "bytes"),</li>
     * <li>scenario(prim(NULL), prim(NULL).bytes, ByteArray, "bytes"),</li>
     * <p>
     * <li>scenario(prim(FULL), prim(FULL).bytes, ByteArray, "bytes[any]"),</li>
     * <li>scenario(prim(NONE), prim(NONE).bytes, ByteArray, "bytes[any]"),</li>
     * <li>scenario(prim(NULL), prim(NULL).bytes, ByteArray, "bytes[any]"),</li>
     * </ul>
     * <p>
     * group B:
     * <ul>
     * <li>scenario(prim(FULL), prim(FULL).bytes[0], Byte, "bytes[0]"),</li>
     * <li>scenario(prim(FULL), prim(FULL).bytes[1], Byte, "bytes[1]"),</li>
     * <li>scenario(prim(FULL), prim(FULL).bytes[2], Byte, "bytes[2]"),</li>
     * <p>
     * <li>for all primitives <ul>
     * <li>scenario(prim(NONE), null, Byte, "bytes[0]"),</li>
     * <li>scenario(prim(NULL), null, Byte, "bytes[1]"),</li>
     * </ul></li>
     * <p>
     * </ul>
     */
    private static Collection<Object[]> expandPrimitiveArrayScenario(Object input, PrimitiveObject result,
                                                                     String pathToExplode, String parent) {
        List<Object[]> scenarios = new ArrayList<>();
        // group A:
        for (CompactValueReaderTestStructure.PrimitiveFields primitiveFields : getPrimitiveArrays()) {
            String path = pathToExplode.replace("primitiveArray", primitiveFields.field);
            Object resultToMatch = result != null ? result.getPrimitiveArray(primitiveFields) : null;
            Object resultToMatchAny = resultToMatch;
            if (resultToMatchAny != null && Array.getLength(resultToMatchAny) == 0) {
                resultToMatchAny = null;
            }

            scenarios.addAll(asList(
                    scenario(input, resultToMatch, path, parent),
                    scenario(input, resultToMatchAny, path + "[any]", parent)
            ));
        }

        // group B:
        for (CompactValueReaderTestStructure.PrimitiveFields primitiveFields : getPrimitives()) {
            String path = pathToExplode.replace("primitiveArray", primitiveFields.field).replace("_", "s");
            if (result == null
                    || result.getPrimitiveArray(primitiveFields) == null
                    || Array.getLength(result.getPrimitiveArray(primitiveFields)) == 0) {
                scenarios.add(scenario(input, null, path + "[0]", parent));
            } else {
                scenarios.addAll(asList(
                        scenario(input, Array.get(result.getPrimitiveArray(primitiveFields), 0), path + "[0]", parent),
                        scenario(input, Array.get(result.getPrimitiveArray(primitiveFields), 1), path + "[1]", parent),
                        scenario(input, Array.get(result.getPrimitiveArray(primitiveFields), 2), path + "[2]", parent)
                ));

            }
        }
        return scenarios;
    }

    /**
     * Expands test cases for that navigate from object array to a primitive field.
     * Word objectArray is replaced to: objects[0], objects[1], objects[2], objects[any]
     * Word "primitive_" is replaced by each primitive type and the scenario is expanded to for each type:
     * <p>
     * A.) The contract is that input should somewhere on the path contain an array of Object[] which contains objects of type
     * PrimitiveObject. For example: "objectArray.primitive_" will be expanded two-fold, the object array and primitive
     * types will be expanded as follows:
     * <ul>
     * <li>objects[0].byte, objects[0].short, ... for all primitive types</li>
     * <li>objects[1].byte, objects[1].short, ...</li>
     * <li>objects[2].byte, objects[2].short, ...</li>
     * </ul>
     * <p>
     * B.) Then the [any] case will be expanded too:
     * <ul>
     * <li>objects[any].byte, objects[any].short, ... for all primitive types</li>
     * </ul>
     * <p>
     * The expected result should be the object that contains the object array - that's the general contract.
     * The result for assertion will be automatically calculated
     */
    @SuppressWarnings({"unchecked"})
    private static Collection<Object[]> expandObjectArrayPrimitiveScenario(Object input, GroupObject result,
                                                                           String pathToExplode, String parent) {
        List<Object[]> scenarios = new ArrayList<>();
        // expansion of the object array using the following quantifiers
        for (String token : asList("0", "1", "2", "any")) {

            String path = pathToExplode.replace("objectArray", "objects[" + token + "]");
            if (token.equals("any")) {
                // B. case with [any] operator on object array
                // expansion of the primitive fields
                for (CompactValueReaderTestStructure.PrimitiveFields primitiveFields : getPrimitives()) {
                    List resultToMatch = new ArrayList();
                    int objectCount = 0;
                    try {
                        objectCount = result.objects.length;
                    } catch (NullPointerException ignored) {
                    }
                    for (int i = 0; i < objectCount; i++) {
                        PrimitiveObject object = (PrimitiveObject) result.objects[i];
                        resultToMatch.add(object.getPrimitive(primitiveFields));
                    }
                    if (result == null || result.objects == null || result.objects.length == 0) {
                        resultToMatch = null;
                    }

                    scenarios.add(scenario(input, resultToMatch, path.replace("primitive_", primitiveFields.field), parent));
                }
            } else {
                // A. case with [0], [1], [2] operator on object array
                // expansion of the primitive fields
                for (PrimitiveFields primitiveFields : getPrimitives()) {
                    Object resultToMatch = null;
                    try {
                        PrimitiveObject object = result.objects[Integer.parseInt(token)];
                        resultToMatch = object.getPrimitive(primitiveFields);
                    } catch (NullPointerException ignored) {
                    } catch (IndexOutOfBoundsException ignored) {
                    }

                    if (result == null || result.objects == null || result.objects.length == 0) {
                        resultToMatch = null;
                    }

                    scenarios.add(scenario(input, resultToMatch, path.replace("primitive_", primitiveFields.field), parent));
                }
            }
        }
        return scenarios;
    }

    // ----------------------------------------------------------------------------------------------------------
    // DIRECT primitive and primitive-array access
    // ----------------------------------------------------------------------------------------------------------
    private static void directPrimitiveScenarios(List<Object[]> result) {
        String parent = "directPrimitiveScenarios";
        // FULLy initialised primitive objects accessed directly
        result.addAll(expandPrimitiveScenario(prim(FULL), prim(FULL), "primitive_", parent));

        // primitive arrays accessed directly (arrays are fully initialised, empty and null)
        result.addAll(expandPrimitiveArrayScenario(prim(FULL), prim(FULL), "primitiveArray", parent));
        result.addAll(expandPrimitiveArrayScenario(prim(NONE), prim(NONE), "primitiveArray", parent));
        result.addAll(expandPrimitiveArrayScenario(prim(NULL), prim(NULL), "primitiveArray", parent));
    }

    // ----------------------------------------------------------------------------------------------------------
    // from PORTABLE to primitive and primitive-array access
    // ----------------------------------------------------------------------------------------------------------
    private static void fromObjectToPrimitiveScenarios(List<Object[]> result) {
        String parent = "directPrimitiveScenariosWrongMethodType";
        // FULLy initialised primitive objects accessed from object
        result.addAll(expandPrimitiveScenario(group(prim(FULL)), prim(FULL), "object.primitive_", parent));

        // primitive arrays accessed from object (arrays are fully initialised, empty and null)
        result.addAll(expandPrimitiveArrayScenario(group(prim(FULL)), prim(FULL), "object.primitiveArray", parent));
        result.addAll(expandPrimitiveArrayScenario(group(prim(NONE)), prim(NONE), "object.primitiveArray", parent));
        result.addAll(expandPrimitiveArrayScenario(group(prim(NULL)), prim(NULL), "object.primitiveArray", parent));
    }

    // ----------------------------------------------------------------------------------------------------------
    // from PORTABLE-ARRAY to primitive and primitive-array access
    // ----------------------------------------------------------------------------------------------------------
    private static void fromObjectArrayToPrimitiveScenarios(List<Object[]> result) {
        String p = "fromObjectArrayToPrimitiveScenarios";

        // FULLy initialised primitive objects accessed from object stored in array
        GroupObject fullGroupVarious = group(prim(1, FULL), prim(10, FULL), prim(100, FULL));
        result.addAll(expandObjectArrayPrimitiveScenario(fullGroupVarious, fullGroupVarious,
                "objectArray.primitive_", p));

        GroupObject fullEmptyNullGroup = group(prim(1, FULL), prim(10, NONE), prim(100, NULL));
        result.addAll(expandObjectArrayPrimitiveScenario(fullEmptyNullGroup, fullEmptyNullGroup,
                "objectArray.primitive_", p));

        // empty or null object array de-referenced further
        GroupObject nullArrayGroup = new GroupObject((PrimitiveObject[]) null);
        result.addAll(expandObjectArrayPrimitiveScenario(nullArrayGroup, nullArrayGroup,
                "objectArray.primitive_", p));

        GroupObject emptyArrayGroup = new GroupObject(new PrimitiveObject[0]);
        result.addAll(expandObjectArrayPrimitiveScenario(emptyArrayGroup, emptyArrayGroup,
                "objectArray.primitive_", p));

        // FULLy initialised primitive arrays accessed from object stored in array
        GroupObject fullGroup = group(prim(FULL), prim(FULL), prim(FULL));
        result.addAll(expandPrimitiveArrayScenario(fullGroup, prim(FULL), "objects[0].primitiveArray", p));
        result.addAll(expandPrimitiveArrayScenario(fullGroup, prim(FULL), "objects[1].primitiveArray", p));
        result.addAll(expandPrimitiveArrayScenario(fullGroup, prim(FULL), "objects[2].primitiveArray", p));

        // EMPTY primitive arrays accessed from object stored in array
        GroupObject noneGroup = group(prim(NONE), prim(NONE), prim(NONE));
        result.addAll(expandPrimitiveArrayScenario(noneGroup, prim(NONE), "objects[0].primitiveArray", p));
        result.addAll(expandPrimitiveArrayScenario(noneGroup, prim(NONE), "objects[1].primitiveArray", p));
        result.addAll(expandPrimitiveArrayScenario(noneGroup, prim(NONE), "objects[2].primitiveArray", p));

        // NULL primitive arrays accessed from object stored in array
        GroupObject nullGroup = group(prim(NULL), prim(NULL), prim(NULL));
        result.addAll(expandPrimitiveArrayScenario(nullGroup, prim(NULL), "objects[0].primitiveArray", p));
        result.addAll(expandPrimitiveArrayScenario(nullGroup, prim(NULL), "objects[1].primitiveArray", p));
        result.addAll(expandPrimitiveArrayScenario(nullGroup, prim(NULL), "objects[2].primitiveArray", p));

        // EMPTY object array -> de-referenced further for primitive access
        result.addAll(expandPrimitiveScenario(emptyArrayGroup, null, "objects[0].primitive_", p));
        result.addAll(expandPrimitiveScenario(emptyArrayGroup, null, "objects[1].primitive_", p));
        result.addAll(expandPrimitiveScenario(emptyArrayGroup, null, "objects[2].primitive_", p));
        result.add(scenario(emptyArrayGroup, null, "objects[0].string_", p));
        result.add(scenario(emptyArrayGroup, null, "objects[1].string_", p));

        // EMPTY object array -> de-referenced further for array access
        result.addAll(expandPrimitiveArrayScenario(emptyArrayGroup, null, "objects[0].primitiveArray", p));
        result.addAll(expandPrimitiveArrayScenario(emptyArrayGroup, null, "objects[1].primitiveArray", p));
        result.addAll(expandPrimitiveArrayScenario(emptyArrayGroup, null, "objects[2].primitiveArray", p));

        // NULL object array -> de-referenced further for primitive access
        result.addAll(expandPrimitiveScenario(nullArrayGroup, null, "objects[0].primitive_", p));
        result.addAll(expandPrimitiveScenario(nullArrayGroup, null, "objects[1].primitive_", p));
        result.addAll(expandPrimitiveScenario(nullArrayGroup, null, "objects[2].primitive_", p));
        result.add(scenario(nullArrayGroup, null, "objects[0].string_", p));
        result.add(scenario(nullArrayGroup, null, "objects[1].string_", p));

        // EMPTY object array -> de-referenced further for array access
        result.addAll(expandPrimitiveArrayScenario(nullArrayGroup, null, "objects[0].primitiveArray", p));
        result.addAll(expandPrimitiveArrayScenario(nullArrayGroup, null, "objects[1].primitiveArray", p));
        result.addAll(expandPrimitiveArrayScenario(nullArrayGroup, null, "objects[2].primitiveArray", p));
    }

    // ----------------------------------------------------------------------------------------------------------
    // from PORTABLE via PORTABLE to further access
    // ----------------------------------------------------------------------------------------------------------
    private static void fromObjectToObjectToPrimitiveScenarios(List<Object[]> result) {
        String p = "fromObjectToObjectToPrimitiveScenarios";
        // FULLy initialised primitive objects accessed from object stored in array
        NestedGroupObject nestedFullGroup = nested(group(prim(1, FULL), prim(10, FULL), prim(100, FULL)));
        result.addAll(asList(
                scenario(nestedFullGroup, (nestedFullGroup.object),
                        "object", p),
                scenario(nestedFullGroup, ((GroupObject) (nestedFullGroup.object)).object,
                        "object.object", p)
        ));
        result.addAll(expandPrimitiveScenario(nestedFullGroup, ((GroupObject) nestedFullGroup.object).object,
                "object.object.primitive_", p));
        result.addAll(expandPrimitiveArrayScenario(nestedFullGroup,
                (PrimitiveObject) ((GroupObject) nestedFullGroup.object).object,
                "object.object.primitiveArray", p));

        NestedGroupObject nestedFullEmptyNullGroup = nested(group(prim(1, FULL), prim(10, NONE), prim(100, NULL)));
        result.addAll(expandPrimitiveScenario(nestedFullEmptyNullGroup,
                ((GroupObject) nestedFullEmptyNullGroup.object).object,
                "object.object.primitive_", p));
        result.addAll(expandPrimitiveArrayScenario(nestedFullEmptyNullGroup,
                (PrimitiveObject) ((GroupObject) nestedFullEmptyNullGroup.object).object,
                "object.object.primitiveArray", p));

        // empty or null object array de-referenced further
        NestedGroupObject nestedNullArrayGroup = nested(new GroupObject((PrimitiveObject[]) null));
        result.addAll(asList(
                scenario(nestedNullArrayGroup, (nestedNullArrayGroup.object), "object", p),
                scenario(nestedNullArrayGroup, null, "object.object", p)
        ));
        result.addAll(expandPrimitiveScenario(nestedNullArrayGroup, null, "object.object.primitive_", p));
        result.addAll(expandPrimitiveArrayScenario(nestedNullArrayGroup, null, "object.object.primitiveArray", p));


        NestedGroupObject nestedNull = nested(new GroupObject[0]);
        result.addAll(asList(
                scenario(nestedNull, null, "object", p),
                scenario(nestedNull, null, "object.object", p)
        ));
        result.addAll(expandPrimitiveScenario(nestedNull, null, "object.object.primitive_", p));
        result.addAll(expandPrimitiveArrayScenario(nestedNull, null, "object.object.primitiveArray", p));
    }

    // ----------------------------------------------------------------------------------------------------------
    // from PORTABLE via PORTABLE_ARRAY to further access
    // ----------------------------------------------------------------------------------------------------------
    private static void fromObjectToObjectArrayToPrimitiveScenarios(List<Object[]> result) {
        // FULLy initialised primitive objects accessed from object stored in array
        String p = "fromObjectToObjectToPrimitiveScenarios";
        NestedGroupObject nestedFullGroup = nested(group(prim(1, FULL), prim(10, FULL), prim(100, FULL)));

        result.addAll(asList(
                scenario(nestedFullGroup, ((GroupObject) (nestedFullGroup.object)).objects,
                        "object.objects", p),
                scenario(nestedFullGroup, ((GroupObject) (nestedFullGroup.object)).objects,
                        "object.objects[any]", p),
                scenario(nestedFullGroup, ((GroupObject) (nestedFullGroup.object)).objects[0],
                        "object.objects[0]", p),
                scenario(nestedFullGroup, ((GroupObject) (nestedFullGroup.object)).objects[1],
                        "object.objects[1]", p),
                scenario(nestedFullGroup, ((GroupObject) (nestedFullGroup.object)).objects[2],
                        "object.objects[2]", p),
                scenario(nestedFullGroup, null, "object.objects[12]", p)
        ));
        result.addAll(expandObjectArrayPrimitiveScenario(nestedFullGroup, (GroupObject) nestedFullGroup.object,
                "object.objectArray.primitive_", p)
        );


        NestedGroupObject nestedFullEmptyNullGroup = nested(group(prim(1, FULL), prim(10, NONE), prim(100, NULL)));
        result.addAll(expandObjectArrayPrimitiveScenario(nestedFullEmptyNullGroup,
                (GroupObject) nestedFullEmptyNullGroup.object, "object.objectArray.primitive_", p)
        );

        // empty or null object array de-referenced further
        NestedGroupObject nestedNullArrayGroup = nested(new GroupObject((PrimitiveObject[]) null));

        result.addAll(asList(
                scenario(nestedNullArrayGroup, null, "object.objects", p),
                scenario(nestedNullArrayGroup, null, "object.objects[any]", p),
                scenario(nestedNullArrayGroup, null, "object.objects[0]", p),
                scenario(nestedNullArrayGroup, null, "object.objects[1]", p),
                scenario(nestedNullArrayGroup, null, "object.objects[2]", p)
        ));
        result.addAll(expandObjectArrayPrimitiveScenario(nestedNullArrayGroup, (GroupObject) nestedNullArrayGroup.object,
                "object.objectArray.primitive_", p)
        );

        NestedGroupObject nestedEmptyArrayGroup = nested(new GroupObject(new PrimitiveObject[0]));

        result.addAll(asList(
                scenario(nestedEmptyArrayGroup, new PrimitiveObject[0], "object.objects", p),
                scenario(nestedEmptyArrayGroup, null, "object.objects[any]", p),
                scenario(nestedEmptyArrayGroup, null, "object.objects[0]", p),
                scenario(nestedEmptyArrayGroup, null, "object.objects[1]", p),
                scenario(nestedEmptyArrayGroup, null, "object.objects[2]", p)
        ));
        result.addAll(expandObjectArrayPrimitiveScenario(nestedEmptyArrayGroup,
                (GroupObject) nestedEmptyArrayGroup.object, "object.objectArray.primitive_", p)
        );

        NestedGroupObject nestedEmpty = nested(new GroupObject[0]);

        result.addAll(asList(
                scenario(nestedEmpty, null, "object.objects", p),
                scenario(nestedEmpty, null, "object.objects[any]", p),
                scenario(nestedEmpty, null, "object.objects[0]", p),
                scenario(nestedEmpty, null, "object.objects[1]", p),
                scenario(nestedEmpty, null, "object.objects[2]", p)
        ));
        result.addAll(expandObjectArrayPrimitiveScenario(nestedEmpty, (GroupObject) nestedEmpty.object,
                "object.objectArray.primitive_", p)
        );

        NestedGroupObject nestedNull = nested((GroupObject[]) null);

        result.addAll(asList(
                scenario(nestedNull, null, "object.objects", p),
                scenario(nestedNull, null, "object.objects[any]", p),
                scenario(nestedNull, null, "object.objects[0]", p),
                scenario(nestedNull, null, "object.objects[1]", p),
                scenario(nestedNull, null, "object.objects[2]", p)
        ));
        result.addAll(expandObjectArrayPrimitiveScenario(nestedNull, (GroupObject) nestedNull.object,
                "object.objectArray.primitive_", p)
        );
    }

    // ----------------------------------------------------------------------------------------------------------
    // from PORTABLE_ARRAY[any] via PORTABLE_ARRAY[any] to further PRIMITIVE access
    // ----------------------------------------------------------------------------------------------------------
    private static void fromObjectArrayAnyToObjectArrayAnyToPrimitiveScenarios(List<Object[]> result) {
        String p = "fromObjectArrayAnyToObjectArrayAnyToPrimitiveScenarios";
        // =============================================
        // INPUT mixed
        // =============================================
        PrimitiveObject p1 = prim(1, NONE);
        PrimitiveObject p10 = prim(10, FULL);
        PrimitiveObject p20 = prim(20, FULL);
        NestedGroupObject input = nested(
                new GroupObject[]{
                        new GroupObject(new PrimitiveObject[0]),
                        group(p1, p10),
                        new GroupObject((PrimitiveObject[]) null),
                        group(new PrimitiveObject[]{p20}),
                }
        );

        result.addAll(asList(
                scenario(input, list(null, p1.byte_, p10.byte_, p20.byte_),
                        "objects[any].objects[any].byte_", p),
                scenario(input, list(null, p1.short_, p10.short_, p20.short_),
                        "objects[any].objects[any].short_", p),
                scenario(input, list(null, p1.int_, p10.int_, p20.int_),
                        "objects[any].objects[any].int_", p),
                scenario(input, list(null, p1.long_, p10.long_, p20.long_),
                        "objects[any].objects[any].long_", p),
                scenario(input, list(null, p1.float_, p10.float_, p20.float_),
                        "objects[any].objects[any].float_", p),
                scenario(input, list(null, p1.double_, p10.double_, p20.double_),
                        "objects[any].objects[any].double_", p),
                scenario(input, list(null, p1.boolean_, p10.boolean_, p20.boolean_),
                        "objects[any].objects[any].boolean_", p),
                scenario(input, list(null, p1.string_, p10.string_, p20.string_),
                        "objects[any].objects[any].string_", p)
        ));

        // =============================================
        // INPUT empty
        // =============================================
        NestedGroupObject inputEmpty = nested(
                new GroupObject[0]
        );

        result.addAll(asList(
                scenario(inputEmpty, null, "objects[any].objects[any].byte_", p),
                scenario(inputEmpty, null, "objects[any].objects[any].short_", p),
                scenario(inputEmpty, null, "objects[any].objects[any].int_", p),
                scenario(inputEmpty, null, "objects[any].objects[any].long_", p),
                scenario(inputEmpty, null, "objects[any].objects[any].float_", p),
                scenario(inputEmpty, null, "objects[any].objects[any].double_", p),
                scenario(inputEmpty, null, "objects[any].objects[any].boolean_", p),
                scenario(inputEmpty, null, "objects[any].objects[any].string_", p)
        ));

        // =============================================
        // INPUT null
        // =============================================
        NestedGroupObject inputNull = nested((GroupObject[]) null);


        result.addAll(asList(
                scenario(inputNull, null, "objects[any].objects[any].byte_", p),
                scenario(inputNull, null, "objects[any].objects[any].short_", p),
                scenario(inputNull, null, "objects[any].objects[any].int_", p),
                scenario(inputNull, null, "objects[any].objects[any].long_", p),
                scenario(inputNull, null, "objects[any].objects[any].float_", p),
                scenario(inputNull, null, "objects[any].objects[any].double_", p),
                scenario(inputNull, null, "objects[any].objects[any].boolean_", p),
                scenario(inputNull, null, "objects[any].objects[any].string_", p)
        ));
    }

    // ----------------------------------------------------------------------------------------------------------
    // from PORTABLE_ARRAY[any] via PORTABLE_ARRAY[any] to further PRIMITIVE_ARRAY[any] access
    // ----------------------------------------------------------------------------------------------------------
    private static void fromObjectArrayToObjectArrayToPrimitiveArrayAnyScenarios(List<Object[]> result) {
        String method = "fromObjectArrayToObjectArrayToPrimitiveArrayAnyScenarios";
        String p = method + " mixed";
        // =============================================
        // INPUT mixed
        // =============================================
        NestedGroupObject input = nested(
                new GroupObject[]{
                        new GroupObject(new PrimitiveObject[0]),
                        group(prim(1, NONE), prim(10, FULL), prim(50, NULL)),
                        new GroupObject((PrimitiveObject[]) null),
                        group(prim(20, FULL), prim(70, NULL)),
                }
        );

        PrimitiveObject p10 = prim(10, FULL);
        PrimitiveObject p20 = prim(20, FULL);

        result.addAll(asList(
                scenario(input, list(null, p10.bytes, p20.bytes),
                        "objects[any].objects[any].bytes[any]", p),
                scenario(input, list(null, p10.shorts, p20.shorts),
                        "objects[any].objects[any].shorts[any]", p),
                scenario(input, list(null, p10.ints, p20.ints),
                        "objects[any].objects[any].ints[any]", p),
                scenario(input, list(null, p10.longs, p20.longs),
                        "objects[any].objects[any].longs[any]", p),
                scenario(input, list(null, p10.floats, p20.floats),
                        "objects[any].objects[any].floats[any]", p),
                scenario(input, list(null, p10.doubles, p20.doubles),
                        "objects[any].objects[any].doubles[any]", p),
                scenario(input, list(null, p10.booleans, p20.booleans),
                        "objects[any].objects[any].booleans[any]", p),
                scenario(input, list(null, p10.strings, p20.strings),
                        "objects[any].objects[any].strings[any]", p)
        ));

        // =============================================
        // INPUT empty
        // =============================================
        p = method + " empty";
        NestedGroupObject inputEmpty = nested(
                new GroupObject[0]
        );

        result.addAll(asList(
                scenario(inputEmpty, null, "objects[any].objects[any].bytes[any]", p),
                scenario(inputEmpty, null, "objects[any].objects[any].shorts[any]", p),
                scenario(inputEmpty, null, "objects[any].objects[any].ints[any]", p),
                scenario(inputEmpty, null, "objects[any].objects[any].longs[any]", p),
                scenario(inputEmpty, null, "objects[any].objects[any].floats[any]", p),
                scenario(inputEmpty, null, "objects[any].objects[any].doubles[any]", p),
                scenario(inputEmpty, null, "objects[any].objects[any].booleans[any]", p),
                scenario(inputEmpty, null, "objects[any].objects[any].strings[any]", p)
        ));

        // =============================================
        // INPUT null
        // =============================================
        p = method + " null";
        NestedGroupObject inputNull = nested((GroupObject[]) null);
        result.addAll(asList(
                scenario(inputNull, null, "objects[any].objects[any].bytes[any]", p),
                scenario(inputNull, null, "objects[any].objects[any].shorts[any]", p),
                scenario(inputNull, null, "objects[any].objects[any].ints[any]", p),
                scenario(inputNull, null, "objects[any].objects[any].longs[any]", p),
                scenario(inputNull, null, "objects[any].objects[any].floats[any]", p),
                scenario(inputNull, null, "objects[any].objects[any].doubles[any]", p),
                scenario(inputNull, null, "objects[any].objects[any].booleans[any]", p),
                scenario(inputNull, null, "objects[any].objects[any].strings[any]", p)
        ));
    }

    // ----------------------------------------------------------------------------------------------------------
    // from PORTABLE_ARRAY to PORTABLE_ARRAY access + further
    // ----------------------------------------------------------------------------------------------------------
    private static void fromObjectArrayToObjectArrayToPrimitiveScenarios(List<Object[]> result) {
        String p = "fromObjectArrayToObjectArrayToPrimitiveScenarios";
        // FULLy initialised primitive objects accessed from object stored in array
        NestedGroupObject nestedFullGroup = nested(group(prim(1, FULL), prim(10, FULL), prim(100, FULL)));

        result.addAll(asList(
                scenario(nestedFullGroup, ((GroupObject) (nestedFullGroup.objects[0])).objects,
                        "objects[0].objects", p),
                scenario(nestedFullGroup, ((GroupObject) (nestedFullGroup.objects[0])).objects,
                        "objects[0].objects[any]", p),
                scenario(nestedFullGroup, prim(1, FULL),
                        "objects[any].objects[0]", p),
                scenario(nestedFullGroup, ((GroupObject) (nestedFullGroup.objects[0])).objects,
                        "objects[any].objects[any]", p),
                scenario(nestedFullGroup, ((GroupObject) (nestedFullGroup.objects[0])).objects[0],
                        "objects[0].objects[0]", p),
                scenario(nestedFullGroup, ((GroupObject) (nestedFullGroup.objects[0])).objects[1],
                        "objects[0].objects[1]", p),
                scenario(nestedFullGroup, ((GroupObject) (nestedFullGroup.objects[0])).objects[2],
                        "objects[0].objects[2]", p),
                scenario(nestedFullGroup, null,
                        "objects[0].objects[12]", p)
        ));

        result.addAll(expandObjectArrayPrimitiveScenario(nestedFullGroup, (GroupObject) nestedFullGroup.object,
                "objects[0].objectArray.primitive_", p)
        );

        NestedGroupObject anyGroup = nested(new GroupObject[]{
                group(NULL), group(NONE), group(prim(1, FULL), prim(10, NONE), prim(50, NULL)), group(prim(2, FULL), prim(20, NONE), prim(80, NULL)),
        });
        result.addAll(expandObjectArrayPrimitiveScenario(anyGroup, (GroupObject) anyGroup.objects[0],
                "objects[0].objectArray.primitive_", p)
        );

        // empty or null object array de-referenced further
        NestedGroupObject nestedNullArrayGroup = nested(new GroupObject((PrimitiveObject[]) null));

        result.addAll(asList(
                scenario(nestedNullArrayGroup, null, "objects[0].objects", p),
                scenario(nestedNullArrayGroup, null, "objects[0].objects[any]", p),
                scenario(nestedNullArrayGroup, null, "objects[any].objects[0]", p),
                scenario(nestedNullArrayGroup, null, "objects[any].objects[any]", p),
                scenario(nestedNullArrayGroup, null, "objects[0].objects[0]", p),
                scenario(nestedNullArrayGroup, null, "objects[0].objects[1]", p),
                scenario(nestedNullArrayGroup, null, "objects[0].objects[2]", p)
        ));
        result.addAll(expandObjectArrayPrimitiveScenario(nestedNullArrayGroup,
                (GroupObject) nestedNullArrayGroup.object, "objects[0].objectArray.primitive_", p)
        );

        NestedGroupObject nestedEmptyArrayGroup = nested(new GroupObject(new PrimitiveObject[0]));

        result.addAll(asList(
                scenario(nestedEmptyArrayGroup, new PrimitiveObject[0], "objects[0].objects", p),
                scenario(nestedEmptyArrayGroup, null, "objects[0].objects[any]", p),
                scenario(nestedEmptyArrayGroup, null, "objects[any].objects[0]", p),
                scenario(nestedEmptyArrayGroup, null, "objects[any].objects[any]", p),
                scenario(nestedEmptyArrayGroup, null, "objects[0].objects[0]", p),
                scenario(nestedEmptyArrayGroup, null, "objects[0].objects[1]", p),
                scenario(nestedEmptyArrayGroup, null, "objects[0].objects[2]", p)
        ));
        result.addAll(expandObjectArrayPrimitiveScenario(nestedEmptyArrayGroup,
                (GroupObject) nestedEmptyArrayGroup.object, "objects[0].objectArray.primitive_", p)
        );

        NestedGroupObject nestedEmpty = nested(new GroupObject[0]);

        result.addAll(asList(
                scenario(nestedEmpty, null, "objects[0].objects", p),
                scenario(nestedEmpty, null, "objects[0].objects[any]", p),
                scenario(nestedEmpty, null, "objects[any].objects[0]", p),
                scenario(nestedEmpty, null, "objects[any].objects[any]", p),
                scenario(nestedEmpty, null, "objects[0].objects[0]", p),
                scenario(nestedEmpty, null, "objects[0].objects[1]", p),
                scenario(nestedEmpty, null, "objects[0].objects[2]", p)
        ));
        result.addAll(expandObjectArrayPrimitiveScenario(nestedEmpty,
                (GroupObject) nestedEmpty.object, "objects[0].objectArray.primitive_", p)
        );

        NestedGroupObject nestedNull = nested((GroupObject[]) null);

        result.addAll(asList(
                scenario(nestedNull, null, "objects[0].objects", p),
                scenario(nestedNull, null, "objects[0].objects[any]", p),
                scenario(nestedNull, null, "objects[any].objects[0]", p),
                scenario(nestedNull, null, "objects[any].objects[any]", p),
                scenario(nestedNull, null, "objects[0].objects[0]", p),
                scenario(nestedNull, null, "objects[0].objects[1]", p),
                scenario(nestedNull, null, "objects[0].objects[2]", p)
        ));
        result.addAll(expandObjectArrayPrimitiveScenario(nestedNull,
                (GroupObject) nestedNull.object, "objects[0].objectArray.primitive_", p)
        );
    }

    // ----------------------------------------------------------------------------------------------------------
    // from PORTABLE_ARRAY to PORTABLE_ARRAY access + further
    // ----------------------------------------------------------------------------------------------------------
    private static void fromObjectArrayToObjectArrayAnyScenarios(List<Object[]> result) {
        String p = "fromObjectArrayToObjectArrayAnyScenarios";
        NestedGroupObject anyGroup = nested(new GroupObject[]{
                group(prim(1, FULL), prim(10, NONE), prim(50, NULL)),
                group(prim(2, FULL), prim(20, NONE), prim(80, NULL)),
        });

        result.addAll(asList(
                scenario(anyGroup, ((GroupObject) (anyGroup.objects[0])).objects,
                        "objects[0].objects[any]", p),
                scenario(anyGroup, new PrimitiveObject[]{prim(1, FULL), prim(2, FULL)},
                        "objects[any].objects[0]", p),
                scenario(anyGroup, new PrimitiveObject[]{prim(10, FULL), prim(20, FULL)},
                        "objects[any].objects[1]", p),
                scenario(anyGroup, new PrimitiveObject[]{prim(50, FULL), prim(80, FULL)},
                        "objects[any].objects[2]", p),
                scenario(anyGroup, new PrimitiveObject[]{prim(1, FULL), prim(10, FULL), prim(50, FULL), prim(2, FULL), prim(20, FULL),
                        prim(80, FULL),
                }, "objects[any].objects[any]", p)
        ));

        NestedGroupObject nestedEmptyArrayGroup = nested(new GroupObject[]{new GroupObject(new PrimitiveObject[0]),
                group(prim(1, FULL), prim(10, NONE), prim(50, NULL)),
        });

        result.addAll(asList(
                scenario(nestedEmptyArrayGroup, null,
                        "objects[0].objects[any]", p),
                scenario(nestedEmptyArrayGroup, new PrimitiveObject[]{null, prim(1, FULL)},
                        "objects[any].objects[0]", p),
                scenario(nestedEmptyArrayGroup, new PrimitiveObject[]{null, prim(10, FULL)},
                        "objects[any].objects[1]", p),
                scenario(nestedEmptyArrayGroup, new PrimitiveObject[]{null, prim(50, FULL)},
                        "objects[any].objects[2]", p),
                scenario(nestedEmptyArrayGroup, new PrimitiveObject[]{null, prim(1, FULL), prim(10, FULL), prim(50, FULL)},
                        "objects[any].objects[any]", p)
        ));

        NestedGroupObject nestedNullArrayGroup = nested(new GroupObject[]{new GroupObject((PrimitiveObject[]) null),
                group(prim(1, FULL), prim(10, NONE), prim(50, NULL)),
        });

        result.addAll(asList(
                scenario(nestedNullArrayGroup, null,
                        "objects[0].objects[any]", p),
                scenario(nestedNullArrayGroup, new PrimitiveObject[]{null, prim(1, FULL)},
                        "objects[any].objects[0]", p),
                scenario(nestedNullArrayGroup, new PrimitiveObject[]{null, prim(10, FULL)},
                        "objects[any].objects[1]", p),
                scenario(nestedNullArrayGroup, new PrimitiveObject[]{null, prim(50, FULL)},
                        "objects[any].objects[2]", p),
                scenario(nestedNullArrayGroup, new PrimitiveObject[]{null, prim(1, FULL), prim(10, FULL), prim(50, FULL)},
                        "objects[any].objects[any]", p)
        ));
    }

    //
    // Test data structure utilities
    //

    private static Object[] scenario(Object input, Object result, String path, String parent) {
        return new Object[]{input, result, path, parent};
    }

    /**
     * Unwraps input objects if they are arrays or lists and adds all to an output list.
     */
    @SuppressWarnings("unchecked")
    private static <T> List<T> list(T... objects) {
        List<T> result = new ArrayList<>();
        for (T object : objects) {
            if (object == null) {
                //noinspection ConstantConditions
                result.add(object);
            } else if (object.getClass().isArray()) {
                int length = Array.getLength(object);
                for (int i = 0; i < length; i++) {
                    result.add((T) Array.get(object, i));
                }
            } else if (object instanceof Collection) {
                result.addAll((Collection<T>) object);
            } else {
                result.add(object);
            }
        }
        return result;
    }
}
