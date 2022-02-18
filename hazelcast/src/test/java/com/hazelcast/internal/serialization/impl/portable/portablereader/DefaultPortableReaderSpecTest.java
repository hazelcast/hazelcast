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

package com.hazelcast.internal.serialization.impl.portable.portablereader;

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.GenericRecordQueryReader;
import com.hazelcast.nio.serialization.Portable;
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

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.internal.serialization.impl.portable.portablereader.DefaultPortableReaderTestStructure.GroupPortable;
import static com.hazelcast.internal.serialization.impl.portable.portablereader.DefaultPortableReaderTestStructure.NestedGroupPortable;
import static com.hazelcast.internal.serialization.impl.portable.portablereader.DefaultPortableReaderTestStructure.PrimitiveFields;
import static com.hazelcast.internal.serialization.impl.portable.portablereader.DefaultPortableReaderTestStructure.PrimitiveFields.getPrimitiveArrays;
import static com.hazelcast.internal.serialization.impl.portable.portablereader.DefaultPortableReaderTestStructure.PrimitiveFields.getPrimitives;
import static com.hazelcast.internal.serialization.impl.portable.portablereader.DefaultPortableReaderTestStructure.PrimitivePortable;
import static com.hazelcast.internal.serialization.impl.portable.portablereader.DefaultPortableReaderTestStructure.PrimitivePortable.Init.FULL;
import static com.hazelcast.internal.serialization.impl.portable.portablereader.DefaultPortableReaderTestStructure.PrimitivePortable.Init.NONE;
import static com.hazelcast.internal.serialization.impl.portable.portablereader.DefaultPortableReaderTestStructure.PrimitivePortable.Init.NULL;
import static com.hazelcast.internal.serialization.impl.portable.portablereader.DefaultPortableReaderTestStructure.TestPortableFactory;
import static com.hazelcast.internal.serialization.impl.portable.portablereader.DefaultPortableReaderTestStructure.group;
import static com.hazelcast.internal.serialization.impl.portable.portablereader.DefaultPortableReaderTestStructure.nested;
import static com.hazelcast.internal.serialization.impl.portable.portablereader.DefaultPortableReaderTestStructure.prim;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertThat;

/**
 * Tests that verifies the behavior of the DefaultPortableReader.
 * All tests cases are generated, since there's a lot of possible cases due to the long lists of read* method on the reader.
 * <p>
 * The test is parametrised with 4 parameters
 * Each test execution runs one read operation on the reader.
 * <p>
 * The rationale behind these tests is to cover all possible combinations of reads using nested paths and quantifiers
 * (number or any) with all possible portable types. It's impossible to do it manually, since there's 20 supported
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
public class DefaultPortableReaderSpecTest extends HazelcastTestSupport {

    private static final PrimitivePortable P_NON_EMPTY = new PrimitivePortable(0, PrimitivePortable.Init.FULL);
    private static final GroupPortable G_NON_EMPTY = group(FULL);
    private static final NestedGroupPortable N_NON_EMPTY = nested(new Portable[]{G_NON_EMPTY, G_NON_EMPTY});

    @Rule
    public ExpectedException expected = ExpectedException.none();

    // input object
    private Portable inputObject;
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
        fromPortableToPrimitiveScenarios(result);
        fromPortableArrayToPrimitiveScenarios(result);
        fromPortableToPortableToPrimitiveScenarios(result);
        fromPortableToPortableArrayToPrimitiveScenarios(result);
        fromPortableArrayToPortableArrayToPrimitiveArrayAnyScenarios(result);
        fromPortableArrayAnyToPortableArrayAnyToPrimitiveScenarios(result);
        fromPortableArrayToPortableArrayToPrimitiveScenarios(result);
        fromPortableArrayToPortableArrayAnyScenarios(result);

        return result;
    }

    public DefaultPortableReaderSpecTest(Portable inputObject, Object expectedResult, String pathToRead, String parent) {
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

        // assert the condition
        Object result = reader(inputObject).read(pathToRead);
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
            assertThat(result, equalTo(resultToMatch));
        } else {
            assertThat(result, equalTo(resultToMatch));
        }

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
     * <li>scenario(input, result.char_, adjustedPath + "char_"),</li>
     * <li>scenario(input, result.string_, adjustedPath + "string_"),</li>
     * </ul>
     */
    private static Collection<Object[]> expandPrimitiveScenario(Portable input, Object result, String pathToExplode,
                                                                String parent) {
        List<Object[]> scenarios = new ArrayList<>();
        Object adjustedResult;
        for (PrimitiveFields primitiveFields : getPrimitives()) {
            if (result instanceof PrimitivePortable) {
                adjustedResult = ((PrimitivePortable) result).getPrimitive(primitiveFields);
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
    private static Collection<Object[]> expandPrimitiveArrayScenario(Portable input, PrimitivePortable result,
                                                                     String pathToExplode, String parent) {
        List<Object[]> scenarios = new ArrayList<>();
        // group A:
        for (PrimitiveFields primitiveFields : getPrimitiveArrays()) {
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
        for (PrimitiveFields primitiveFields : getPrimitives()) {
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
     * Expands test cases for that navigate from portable array to a primitive field.
     * Word portableArray is replaced to: portables[0], portables[1], portables[2], portables[any]
     * Word "primitive_" is replaced by each primitive type and the scenario is expanded to for each type:
     * <p>
     * A.) The contract is that input should somewhere on the path contain an array of Portable[] which contains objects of type
     * PrimitivePortable. For example: "portableArray.primitive_" will be expanded two-fold, the portable array and primitive
     * types will be expanded as follows:
     * <ul>
     * <li>portables[0].byte, portables[0].short, portables[0].char, ... for all primitive types</li>
     * <li>portables[1].byte, portables[1].short, portables[1].char, ...</li>
     * <li>portables[2].byte, portables[2].short, portables[2].char, ...</li>
     * </ul>
     * <p>
     * B.) Then the [any] case will be expanded too:
     * <ul>
     * <li>portables[any].byte, portables[any].short, portables[any].char, ... for all primitive types</li>
     * </ul>
     * <p>
     * The expected result should be the object that contains the portable array - that's the general contract.
     * The result for assertion will be automatically calculated
     */
    @SuppressWarnings({"unchecked"})
    private static Collection<Object[]> expandPortableArrayPrimitiveScenario(Portable input, GroupPortable result,
                                                                             String pathToExplode, String parent) {
        List<Object[]> scenarios = new ArrayList<>();
        // expansion of the portable array using the following quantifiers
        for (String token : asList("0", "1", "2", "any")) {

            String path = pathToExplode.replace("portableArray", "portables[" + token + "]");
            if (token.equals("any")) {
                // B. case with [any] operator on portable array
                // expansion of the primitive fields
                for (PrimitiveFields primitiveFields : getPrimitives()) {
                    List resultToMatch = new ArrayList();
                    int portableCount = 0;
                    try {
                        portableCount = result.portables.length;
                    } catch (NullPointerException ignored) {
                    }
                    for (int i = 0; i < portableCount; i++) {
                        PrimitivePortable portable = (PrimitivePortable) result.portables[i];
                        resultToMatch.add(portable.getPrimitive(primitiveFields));
                    }
                    if (result == null || result.portables == null || result.portables.length == 0) {
                        resultToMatch = null;
                    }

                    scenarios.add(scenario(input, resultToMatch, path.replace("primitive_", primitiveFields.field), parent));
                }
            } else {
                // A. case with [0], [1], [2] operator on portable array
                // expansion of the primitive fields
                for (PrimitiveFields primitiveFields : getPrimitives()) {
                    Object resultToMatch = null;
                    try {
                        PrimitivePortable portable = (PrimitivePortable) result.portables[Integer.parseInt(token)];
                        resultToMatch = portable.getPrimitive(primitiveFields);
                    } catch (NullPointerException ignored) {
                    } catch (IndexOutOfBoundsException ignored) {
                    }

                    if (result == null || result.portables == null || result.portables.length == 0) {
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
    private static void fromPortableToPrimitiveScenarios(List<Object[]> result) {
        String parent = "directPrimitiveScenariosWrongMethodType";
        // FULLy initialised primitive objects accessed from portable
        result.addAll(expandPrimitiveScenario(group(prim(FULL)), prim(FULL), "portable.primitive_", parent));

        // primitive arrays accessed from portable (arrays are fully initialised, empty and null)
        result.addAll(expandPrimitiveArrayScenario(group(prim(FULL)), prim(FULL), "portable.primitiveArray", parent));
        result.addAll(expandPrimitiveArrayScenario(group(prim(NONE)), prim(NONE), "portable.primitiveArray", parent));
        result.addAll(expandPrimitiveArrayScenario(group(prim(NULL)), prim(NULL), "portable.primitiveArray", parent));
    }

    // ----------------------------------------------------------------------------------------------------------
    // from PORTABLE-ARRAY to primitive and primitive-array access
    // ----------------------------------------------------------------------------------------------------------
    private static void fromPortableArrayToPrimitiveScenarios(List<Object[]> result) {
        String p = "fromPortableArrayToPrimitiveScenarios";

        // FULLy initialised primitive objects accessed from portable stored in array
        GroupPortable fullGroupVarious = group(prim(1, FULL), prim(10, FULL), prim(100, FULL));
        result.addAll(expandPortableArrayPrimitiveScenario(fullGroupVarious, fullGroupVarious,
                "portableArray.primitive_", p));

        GroupPortable fullEmptyNullGroup = group(prim(1, FULL), prim(10, NONE), prim(100, NULL));
        result.addAll(expandPortableArrayPrimitiveScenario(fullEmptyNullGroup, fullEmptyNullGroup,
                "portableArray.primitive_", p));

        // empty or null portable array de-referenced further
        GroupPortable nullArrayGroup = new GroupPortable((Portable[]) null);
        result.addAll(expandPortableArrayPrimitiveScenario(nullArrayGroup, nullArrayGroup,
                "portableArray.primitive_", p));

        GroupPortable emptyArrayGroup = new GroupPortable(new Portable[0]);
        result.addAll(expandPortableArrayPrimitiveScenario(emptyArrayGroup, emptyArrayGroup,
                "portableArray.primitive_", p));

        // FULLy initialised primitive arrays accessed from portable stored in array
        GroupPortable fullGroup = group(prim(FULL), prim(FULL), prim(FULL));
        result.addAll(expandPrimitiveArrayScenario(fullGroup, prim(FULL), "portables[0].primitiveArray", p));
        result.addAll(expandPrimitiveArrayScenario(fullGroup, prim(FULL), "portables[1].primitiveArray", p));
        result.addAll(expandPrimitiveArrayScenario(fullGroup, prim(FULL), "portables[2].primitiveArray", p));

        // EMPTY primitive arrays accessed from portable stored in array
        GroupPortable noneGroup = group(prim(NONE), prim(NONE), prim(NONE));
        result.addAll(expandPrimitiveArrayScenario(noneGroup, prim(NONE), "portables[0].primitiveArray", p));
        result.addAll(expandPrimitiveArrayScenario(noneGroup, prim(NONE), "portables[1].primitiveArray", p));
        result.addAll(expandPrimitiveArrayScenario(noneGroup, prim(NONE), "portables[2].primitiveArray", p));

        // NULL primitive arrays accessed from portable stored in array
        GroupPortable nullGroup = group(prim(NULL), prim(NULL), prim(NULL));
        result.addAll(expandPrimitiveArrayScenario(nullGroup, prim(NULL), "portables[0].primitiveArray", p));
        result.addAll(expandPrimitiveArrayScenario(nullGroup, prim(NULL), "portables[1].primitiveArray", p));
        result.addAll(expandPrimitiveArrayScenario(nullGroup, prim(NULL), "portables[2].primitiveArray", p));

        // EMPTY portable array -> de-referenced further for primitive access
        result.addAll(expandPrimitiveScenario(emptyArrayGroup, null, "portables[0].primitive_", p));
        result.addAll(expandPrimitiveScenario(emptyArrayGroup, null, "portables[1].primitive_", p));
        result.addAll(expandPrimitiveScenario(emptyArrayGroup, null, "portables[2].primitive_", p));
        result.add(scenario(emptyArrayGroup, null, "portables[0].string_", p));
        result.add(scenario(emptyArrayGroup, null, "portables[1].string_", p));

        // EMPTY portable array -> de-referenced further for array access
        result.addAll(expandPrimitiveArrayScenario(emptyArrayGroup, null, "portables[0].primitiveArray", p));
        result.addAll(expandPrimitiveArrayScenario(emptyArrayGroup, null, "portables[1].primitiveArray", p));
        result.addAll(expandPrimitiveArrayScenario(emptyArrayGroup, null, "portables[2].primitiveArray", p));

        // NULL portable array -> de-referenced further for primitive access
        result.addAll(expandPrimitiveScenario(nullArrayGroup, null, "portables[0].primitive_", p));
        result.addAll(expandPrimitiveScenario(nullArrayGroup, null, "portables[1].primitive_", p));
        result.addAll(expandPrimitiveScenario(nullArrayGroup, null, "portables[2].primitive_", p));
        result.add(scenario(nullArrayGroup, null, "portables[0].string_", p));
        result.add(scenario(nullArrayGroup, null, "portables[1].string_", p));

        // EMPTY portable array -> de-referenced further for array access
        result.addAll(expandPrimitiveArrayScenario(nullArrayGroup, null, "portables[0].primitiveArray", p));
        result.addAll(expandPrimitiveArrayScenario(nullArrayGroup, null, "portables[1].primitiveArray", p));
        result.addAll(expandPrimitiveArrayScenario(nullArrayGroup, null, "portables[2].primitiveArray", p));
    }

    // ----------------------------------------------------------------------------------------------------------
    // from PORTABLE via PORTABLE to further access
    // ----------------------------------------------------------------------------------------------------------
    private static void fromPortableToPortableToPrimitiveScenarios(List<Object[]> result) {
        String p = "fromPortableToPortableToPrimitiveScenarios";
        // FULLy initialised primitive objects accessed from portable stored in array
        NestedGroupPortable nestedFullGroup = nested(group(prim(1, FULL), prim(10, FULL), prim(100, FULL)));
        result.addAll(asList(
                scenario(nestedFullGroup, (nestedFullGroup.portable),
                        "portable", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portable)).portable,
                        "portable.portable", p)
        ));
        result.addAll(expandPrimitiveScenario(nestedFullGroup, ((GroupPortable) nestedFullGroup.portable).portable,
                "portable.portable.primitive_", p));
        result.addAll(expandPrimitiveArrayScenario(nestedFullGroup,
                (PrimitivePortable) ((GroupPortable) nestedFullGroup.portable).portable,
                "portable.portable.primitiveArray", p));

        NestedGroupPortable nestedFullEmptyNullGroup = nested(group(prim(1, FULL), prim(10, NONE), prim(100, NULL)));
        result.addAll(expandPrimitiveScenario(nestedFullEmptyNullGroup,
                ((GroupPortable) nestedFullEmptyNullGroup.portable).portable,
                "portable.portable.primitive_", p));
        result.addAll(expandPrimitiveArrayScenario(nestedFullEmptyNullGroup,
                (PrimitivePortable) ((GroupPortable) nestedFullEmptyNullGroup.portable).portable,
                "portable.portable.primitiveArray", p));

        // empty or null portable array de-referenced further
        NestedGroupPortable nestedNullArrayGroup = nested(new GroupPortable((Portable[]) null));
        result.addAll(asList(
                scenario(nestedNullArrayGroup, (nestedNullArrayGroup.portable), "portable", p),
                scenario(nestedNullArrayGroup, null, "portable.portable", p)
        ));
        result.addAll(expandPrimitiveScenario(nestedNullArrayGroup, null, "portable.portable.primitive_", p));
        result.addAll(expandPrimitiveArrayScenario(nestedNullArrayGroup, null, "portable.portable.primitiveArray", p));


        NestedGroupPortable nestedNull = nested(new Portable[0]);
        result.addAll(asList(
                scenario(nestedNull, null, "portable", p),
                scenario(nestedNull, null, "portable.portable", p)
        ));
        result.addAll(expandPrimitiveScenario(nestedNull, null, "portable.portable.primitive_", p));
        result.addAll(expandPrimitiveArrayScenario(nestedNull, null, "portable.portable.primitiveArray", p));
    }

    // ----------------------------------------------------------------------------------------------------------
    // from PORTABLE via PORTABLE_ARRAY to further access
    // ----------------------------------------------------------------------------------------------------------
    private static void fromPortableToPortableArrayToPrimitiveScenarios(List<Object[]> result) {
        // FULLy initialised primitive objects accessed from portable stored in array
        String p = "fromPortableToPortableToPrimitiveScenarios";
        NestedGroupPortable nestedFullGroup = nested(group(prim(1, FULL), prim(10, FULL), prim(100, FULL)));

        result.addAll(asList(
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portable)).portables,
                        "portable.portables", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portable)).portables,
                        "portable.portables[any]", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portable)).portables[0],
                        "portable.portables[0]", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portable)).portables[1],
                        "portable.portables[1]", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portable)).portables[2],
                        "portable.portables[2]", p),
                scenario(nestedFullGroup, null, "portable.portables[12]", p)
        ));
        result.addAll(expandPortableArrayPrimitiveScenario(nestedFullGroup, (GroupPortable) nestedFullGroup.portable,
                "portable.portableArray.primitive_", p)
        );


        NestedGroupPortable nestedFullEmptyNullGroup = nested(group(prim(1, FULL), prim(10, NONE), prim(100, NULL)));
        result.addAll(expandPortableArrayPrimitiveScenario(nestedFullEmptyNullGroup,
                (GroupPortable) nestedFullEmptyNullGroup.portable, "portable.portableArray.primitive_", p)
        );

        // empty or null portable array de-referenced further
        NestedGroupPortable nestedNullArrayGroup = nested(new GroupPortable((Portable[]) null));

        result.addAll(asList(
                scenario(nestedNullArrayGroup, null, "portable.portables", p),
                scenario(nestedNullArrayGroup, null, "portable.portables[any]", p),
                scenario(nestedNullArrayGroup, null, "portable.portables[0]", p),
                scenario(nestedNullArrayGroup, null, "portable.portables[1]", p),
                scenario(nestedNullArrayGroup, null, "portable.portables[2]", p)
        ));
        result.addAll(expandPortableArrayPrimitiveScenario(nestedNullArrayGroup, (GroupPortable) nestedNullArrayGroup.portable,
                "portable.portableArray.primitive_", p)
        );

        NestedGroupPortable nestedEmptyArrayGroup = nested(new GroupPortable(new Portable[0]));

        result.addAll(asList(
                scenario(nestedEmptyArrayGroup, new Portable[0], "portable.portables", p),
                scenario(nestedEmptyArrayGroup, null, "portable.portables[any]", p),
                scenario(nestedEmptyArrayGroup, null, "portable.portables[0]", p),
                scenario(nestedEmptyArrayGroup, null, "portable.portables[1]", p),
                scenario(nestedEmptyArrayGroup, null, "portable.portables[2]", p)
        ));
        result.addAll(expandPortableArrayPrimitiveScenario(nestedEmptyArrayGroup,
                (GroupPortable) nestedEmptyArrayGroup.portable, "portable.portableArray.primitive_", p)
        );

        NestedGroupPortable nestedEmpty = nested(new GroupPortable[0]);

        result.addAll(asList(
                scenario(nestedEmpty, null, "portable.portables", p),
                scenario(nestedEmpty, null, "portable.portables[any]", p),
                scenario(nestedEmpty, null, "portable.portables[0]", p),
                scenario(nestedEmpty, null, "portable.portables[1]", p),
                scenario(nestedEmpty, null, "portable.portables[2]", p)
        ));
        result.addAll(expandPortableArrayPrimitiveScenario(nestedEmpty, (GroupPortable) nestedEmpty.portable,
                "portable.portableArray.primitive_", p)
        );

        NestedGroupPortable nestedNull = nested((GroupPortable[]) null);

        result.addAll(asList(
                scenario(nestedNull, null, "portable.portables", p),
                scenario(nestedNull, null, "portable.portables[any]", p),
                scenario(nestedNull, null, "portable.portables[0]", p),
                scenario(nestedNull, null, "portable.portables[1]", p),
                scenario(nestedNull, null, "portable.portables[2]", p)
        ));
        result.addAll(expandPortableArrayPrimitiveScenario(nestedNull, (GroupPortable) nestedNull.portable,
                "portable.portableArray.primitive_", p)
        );
    }

    // ----------------------------------------------------------------------------------------------------------
    // from PORTABLE_ARRAY[any] via PORTABLE_ARRAY[any] to further PRIMITIVE access
    // ----------------------------------------------------------------------------------------------------------
    private static void fromPortableArrayAnyToPortableArrayAnyToPrimitiveScenarios(List<Object[]> result) {
        String p = "fromPortableArrayAnyToPortableArrayAnyToPrimitiveScenarios";
        // =============================================
        // INPUT mixed
        // =============================================
        PrimitivePortable p1 = prim(1, NONE);
        PrimitivePortable p10 = prim(10, FULL);
        PrimitivePortable p20 = prim(20, FULL);
        NestedGroupPortable input = nested(
                new Portable[]{
                        new GroupPortable(new Portable[0]),
                        group(p1, p10),
                        new GroupPortable((Portable[]) null),
                        group(new PrimitivePortable[]{p20}),
                }
        );

        result.addAll(asList(
                scenario(input, list(null, p1.byte_, p10.byte_, p20.byte_),
                        "portables[any].portables[any].byte_", p),
                scenario(input, list(null, p1.short_, p10.short_, p20.short_),
                        "portables[any].portables[any].short_", p),
                scenario(input, list(null, p1.int_, p10.int_, p20.int_),
                        "portables[any].portables[any].int_", p),
                scenario(input, list(null, p1.long_, p10.long_, p20.long_),
                        "portables[any].portables[any].long_", p),
                scenario(input, list(null, p1.char_, p10.char_, p20.char_),
                        "portables[any].portables[any].char_", p),
                scenario(input, list(null, p1.float_, p10.float_, p20.float_),
                        "portables[any].portables[any].float_", p),
                scenario(input, list(null, p1.double_, p10.double_, p20.double_),
                        "portables[any].portables[any].double_", p),
                scenario(input, list(null, p1.boolean_, p10.boolean_, p20.boolean_),
                        "portables[any].portables[any].boolean_", p),
                scenario(input, list(null, p1.string_, p10.string_, p20.string_),
                        "portables[any].portables[any].string_", p)
        ));

        // =============================================
        // INPUT empty
        // =============================================
        NestedGroupPortable inputEmpty = nested(
                new Portable[0]
        );

        result.addAll(asList(
                scenario(inputEmpty, null, "portables[any].portables[any].byte_", p),
                scenario(inputEmpty, null, "portables[any].portables[any].short_", p),
                scenario(inputEmpty, null, "portables[any].portables[any].int_", p),
                scenario(inputEmpty, null, "portables[any].portables[any].long_", p),
                scenario(inputEmpty, null, "portables[any].portables[any].char_", p),
                scenario(inputEmpty, null, "portables[any].portables[any].float_", p),
                scenario(inputEmpty, null, "portables[any].portables[any].double_", p),
                scenario(inputEmpty, null, "portables[any].portables[any].boolean_", p),
                scenario(inputEmpty, null, "portables[any].portables[any].string_", p)
        ));

        // =============================================
        // INPUT null
        // =============================================
        NestedGroupPortable inputNull = nested((Portable[]) null);


        result.addAll(asList(
                scenario(inputNull, null, "portables[any].portables[any].byte_", p),
                scenario(inputNull, null, "portables[any].portables[any].short_", p),
                scenario(inputNull, null, "portables[any].portables[any].int_", p),
                scenario(inputNull, null, "portables[any].portables[any].long_", p),
                scenario(inputNull, null, "portables[any].portables[any].char_", p),
                scenario(inputNull, null, "portables[any].portables[any].float_", p),
                scenario(inputNull, null, "portables[any].portables[any].double_", p),
                scenario(inputNull, null, "portables[any].portables[any].boolean_", p),
                scenario(inputNull, null, "portables[any].portables[any].string_", p)
        ));
    }

    // ----------------------------------------------------------------------------------------------------------
    // from PORTABLE_ARRAY[any] via PORTABLE_ARRAY[any] to further PRIMITIVE_ARRAY[any] access
    // ----------------------------------------------------------------------------------------------------------
    private static void fromPortableArrayToPortableArrayToPrimitiveArrayAnyScenarios(List<Object[]> result) {
        String method = "fromPortableArrayToPortableArrayToPrimitiveArrayAnyScenarios";
        String p = method + " mixed";
        // =============================================
        // INPUT mixed
        // =============================================
        NestedGroupPortable input = nested(
                new Portable[]{
                        new GroupPortable(new Portable[0]),
                        group(prim(1, NONE), prim(10, FULL), prim(50, NULL)),
                        new GroupPortable((Portable[]) null),
                        group(prim(20, FULL), prim(70, NULL)),
                }
        );

        PrimitivePortable p10 = prim(10, FULL);
        PrimitivePortable p20 = prim(20, FULL);

        result.addAll(asList(
                scenario(input, list(null, p10.bytes, p20.bytes),
                        "portables[any].portables[any].bytes[any]", p),
                scenario(input, list(null, p10.shorts, p20.shorts),
                        "portables[any].portables[any].shorts[any]", p),
                scenario(input, list(null, p10.ints, p20.ints),
                        "portables[any].portables[any].ints[any]", p),
                scenario(input, list(null, p10.longs, p20.longs),
                        "portables[any].portables[any].longs[any]", p),
                scenario(input, list(null, p10.chars, p20.chars),
                        "portables[any].portables[any].chars[any]", p),
                scenario(input, list(null, p10.floats, p20.floats),
                        "portables[any].portables[any].floats[any]", p),
                scenario(input, list(null, p10.doubles, p20.doubles),
                        "portables[any].portables[any].doubles[any]", p),
                scenario(input, list(null, p10.booleans, p20.booleans),
                        "portables[any].portables[any].booleans[any]", p),
                scenario(input, list(null, p10.strings, p20.strings),
                        "portables[any].portables[any].strings[any]", p)
        ));

        // =============================================
        // INPUT empty
        // =============================================
        p = method + " empty";
        NestedGroupPortable inputEmpty = nested(
                new Portable[0]
        );

        result.addAll(asList(
                scenario(inputEmpty, null, "portables[any].portables[any].bytes[any]", p),
                scenario(inputEmpty, null, "portables[any].portables[any].shorts[any]", p),
                scenario(inputEmpty, null, "portables[any].portables[any].ints[any]", p),
                scenario(inputEmpty, null, "portables[any].portables[any].longs[any]", p),
                scenario(inputEmpty, null, "portables[any].portables[any].chars[any]", p),
                scenario(inputEmpty, null, "portables[any].portables[any].floats[any]", p),
                scenario(inputEmpty, null, "portables[any].portables[any].doubles[any]", p),
                scenario(inputEmpty, null, "portables[any].portables[any].booleans[any]", p),
                scenario(inputEmpty, null, "portables[any].portables[any].strings[any]", p)
        ));

        // =============================================
        // INPUT null
        // =============================================
        p = method + " null";
        NestedGroupPortable inputNull = nested((Portable[]) null);
        result.addAll(asList(
                scenario(inputNull, null, "portables[any].portables[any].bytes[any]", p),
                scenario(inputNull, null, "portables[any].portables[any].shorts[any]", p),
                scenario(inputNull, null, "portables[any].portables[any].ints[any]", p),
                scenario(inputNull, null, "portables[any].portables[any].longs[any]", p),
                scenario(inputNull, null, "portables[any].portables[any].chars[any]", p),
                scenario(inputNull, null, "portables[any].portables[any].floats[any]", p),
                scenario(inputNull, null, "portables[any].portables[any].doubles[any]", p),
                scenario(inputNull, null, "portables[any].portables[any].booleans[any]", p),
                scenario(inputNull, null, "portables[any].portables[any].strings[any]", p)
        ));
    }

    // ----------------------------------------------------------------------------------------------------------
    // from PORTABLE_ARRAY to PORTABLE_ARRAY access + further
    // ----------------------------------------------------------------------------------------------------------
    private static void fromPortableArrayToPortableArrayToPrimitiveScenarios(List<Object[]> result) {
        String p = "fromPortableArrayToPortableArrayToPrimitiveScenarios";
        // FULLy initialised primitive objects accessed from portable stored in array
        NestedGroupPortable nestedFullGroup = nested(group(prim(1, FULL), prim(10, FULL), prim(100, FULL)));

        result.addAll(asList(
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portables[0])).portables,
                        "portables[0].portables", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portables[0])).portables,
                        "portables[0].portables[any]", p),
                scenario(nestedFullGroup, prim(1, FULL),
                        "portables[any].portables[0]", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portables[0])).portables,
                        "portables[any].portables[any]", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portables[0])).portables[0],
                        "portables[0].portables[0]", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portables[0])).portables[1],
                        "portables[0].portables[1]", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portables[0])).portables[2],
                        "portables[0].portables[2]", p),
                scenario(nestedFullGroup, null,
                        "portables[0].portables[12]", p)
        ));

        result.addAll(expandPortableArrayPrimitiveScenario(nestedFullGroup, (GroupPortable) nestedFullGroup.portable,
                "portables[0].portableArray.primitive_", p)
        );

        NestedGroupPortable anyGroup = nested(new Portable[]{
                group(prim(1, FULL), prim(10, NONE), prim(50, NULL)), group(prim(2, FULL), prim(20, NONE), prim(80, NULL)),
        });
        result.addAll(expandPortableArrayPrimitiveScenario(anyGroup, (GroupPortable) anyGroup.portables[0],
                "portables[0].portableArray.primitive_", p)
        );

        // empty or null portable array de-referenced further
        NestedGroupPortable nestedNullArrayGroup = nested(new GroupPortable((Portable[]) null));

        result.addAll(asList(
                scenario(nestedNullArrayGroup, null, "portables[0].portables", p),
                scenario(nestedNullArrayGroup, null, "portables[0].portables[any]", p),
                scenario(nestedNullArrayGroup, null, "portables[any].portables[0]", p),
                scenario(nestedNullArrayGroup, null, "portables[any].portables[any]", p),
                scenario(nestedNullArrayGroup, null, "portables[0].portables[0]", p),
                scenario(nestedNullArrayGroup, null, "portables[0].portables[1]", p),
                scenario(nestedNullArrayGroup, null, "portables[0].portables[2]", p)
        ));
        result.addAll(expandPortableArrayPrimitiveScenario(nestedNullArrayGroup,
                (GroupPortable) nestedNullArrayGroup.portable, "portables[0].portableArray.primitive_", p)
        );

        NestedGroupPortable nestedEmptyArrayGroup = nested(new GroupPortable(new Portable[0]));

        result.addAll(asList(
                scenario(nestedEmptyArrayGroup, new Portable[0], "portables[0].portables", p),
                scenario(nestedEmptyArrayGroup, null, "portables[0].portables[any]", p),
                scenario(nestedEmptyArrayGroup, null, "portables[any].portables[0]", p),
                scenario(nestedEmptyArrayGroup, null, "portables[any].portables[any]", p),
                scenario(nestedEmptyArrayGroup, null, "portables[0].portables[0]", p),
                scenario(nestedEmptyArrayGroup, null, "portables[0].portables[1]", p),
                scenario(nestedEmptyArrayGroup, null, "portables[0].portables[2]", p)
        ));
        result.addAll(expandPortableArrayPrimitiveScenario(nestedEmptyArrayGroup,
                (GroupPortable) nestedEmptyArrayGroup.portable, "portables[0].portableArray.primitive_", p)
        );

        NestedGroupPortable nestedEmpty = nested(new GroupPortable[0]);

        result.addAll(asList(
                scenario(nestedEmpty, null, "portables[0].portables", p),
                scenario(nestedEmpty, null, "portables[0].portables[any]", p),
                scenario(nestedEmpty, null, "portables[any].portables[0]", p),
                scenario(nestedEmpty, null, "portables[any].portables[any]", p),
                scenario(nestedEmpty, null, "portables[0].portables[0]", p),
                scenario(nestedEmpty, null, "portables[0].portables[1]", p),
                scenario(nestedEmpty, null, "portables[0].portables[2]", p)
        ));
        result.addAll(expandPortableArrayPrimitiveScenario(nestedEmpty,
                (GroupPortable) nestedEmpty.portable, "portables[0].portableArray.primitive_", p)
        );

        NestedGroupPortable nestedNull = nested((GroupPortable[]) null);

        result.addAll(asList(
                scenario(nestedNull, null, "portables[0].portables", p),
                scenario(nestedNull, null, "portables[0].portables[any]", p),
                scenario(nestedNull, null, "portables[any].portables[0]", p),
                scenario(nestedNull, null, "portables[any].portables[any]", p),
                scenario(nestedNull, null, "portables[0].portables[0]", p),
                scenario(nestedNull, null, "portables[0].portables[1]", p),
                scenario(nestedNull, null, "portables[0].portables[2]", p)
        ));
        result.addAll(expandPortableArrayPrimitiveScenario(nestedNull,
                (GroupPortable) nestedNull.portable, "portables[0].portableArray.primitive_", p)
        );
    }

    // ----------------------------------------------------------------------------------------------------------
    // from PORTABLE_ARRAY to PORTABLE_ARRAY access + further
    // ----------------------------------------------------------------------------------------------------------
    private static void fromPortableArrayToPortableArrayAnyScenarios(List<Object[]> result) {
        String p = "fromPortableArrayToPortableArrayAnyScenarios";
        NestedGroupPortable anyGroup = nested(new Portable[]{
                group(prim(1, FULL), prim(10, NONE), prim(50, NULL)),
                group(prim(2, FULL), prim(20, NONE), prim(80, NULL)),
        });

        result.addAll(asList(
                scenario(anyGroup, ((GroupPortable) (anyGroup.portables[0])).portables,
                        "portables[0].portables[any]", p),
                scenario(anyGroup, new Portable[]{prim(1, FULL), prim(2, FULL)},
                        "portables[any].portables[0]", p),
                scenario(anyGroup, new Portable[]{prim(10, FULL), prim(20, FULL)},
                        "portables[any].portables[1]", p),
                scenario(anyGroup, new Portable[]{prim(50, FULL), prim(80, FULL)},
                        "portables[any].portables[2]", p),
                scenario(anyGroup, new Portable[]{prim(1, FULL), prim(10, FULL), prim(50, FULL), prim(2, FULL), prim(20, FULL),
                        prim(80, FULL),
                }, "portables[any].portables[any]", p)
        ));

        NestedGroupPortable nestedEmptyArrayGroup = nested(new Portable[]{new GroupPortable(new Portable[0]),
                group(prim(1, FULL), prim(10, NONE), prim(50, NULL)),
        });

        result.addAll(asList(
                scenario(nestedEmptyArrayGroup, null,
                        "portables[0].portables[any]", p),
                scenario(nestedEmptyArrayGroup, new Portable[]{null, prim(1, FULL)},
                        "portables[any].portables[0]", p),
                scenario(nestedEmptyArrayGroup, new Portable[]{null, prim(10, FULL)},
                        "portables[any].portables[1]", p),
                scenario(nestedEmptyArrayGroup, new Portable[]{null, prim(50, FULL)},
                        "portables[any].portables[2]", p),
                scenario(nestedEmptyArrayGroup, new Portable[]{null, prim(1, FULL), prim(10, FULL), prim(50, FULL)},
                        "portables[any].portables[any]", p)
        ));

        NestedGroupPortable nestedNullArrayGroup = nested(new Portable[]{new GroupPortable((Portable[]) null),
                group(prim(1, FULL), prim(10, NONE), prim(50, NULL)),
        });

        result.addAll(asList(
                scenario(nestedNullArrayGroup, null,
                        "portables[0].portables[any]", p),
                scenario(nestedNullArrayGroup, new Portable[]{null, prim(1, FULL)},
                        "portables[any].portables[0]", p),
                scenario(nestedNullArrayGroup, new Portable[]{null, prim(10, FULL)},
                        "portables[any].portables[1]", p),
                scenario(nestedNullArrayGroup, new Portable[]{null, prim(50, FULL)},
                        "portables[any].portables[2]", p),
                scenario(nestedNullArrayGroup, new Portable[]{null, prim(1, FULL), prim(10, FULL), prim(50, FULL)},
                        "portables[any].portables[any]", p)
        ));
    }

    //
    // Test data structure utilities
    //

    private static Object[] scenario(Portable input, Object result, String path, String parent) {
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

    //
    // Hazelcast init Utilities
    //

    public GenericRecordQueryReader reader(Portable portable) throws IOException {
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.addPortableFactory(TestPortableFactory.ID, new TestPortableFactory());

        InternalSerializationService ss = new DefaultSerializationServiceBuilder().setConfig(serializationConfig).build();

        // put fully initialised object to a map
        // avoid the case where there's no class definition
        ss.toData(P_NON_EMPTY);
        ss.toData(G_NON_EMPTY);
        ss.toData(N_NON_EMPTY);

        Data data = ss.toData(portable);
        return new GenericRecordQueryReader(ss.readAsInternalGenericRecord(data));
    }

}
