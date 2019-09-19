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

package com.hazelcast.nio.serialization.impl;

import com.hazelcast.config.Config;
import com.hazelcast.map.IMap;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.LazyMapEntry;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.query.impl.getters.MultiResult;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.internal.util.ExceptionUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.GroupPortable;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.Method;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.Method.BooleanArray;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.Method.ByteArray;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.Method.CharArray;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.Method.DoubleArray;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.Method.FloatArray;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.Method.Generic;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.Method.IntArray;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.Method.LongArray;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.Method.Portable;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.Method.PortableArray;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.Method.ShortArray;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.Method.UTF;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.Method.UTFArray;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.Method.getArrayMethodFor;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.Method.getPrimitiveArrays;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.Method.getPrimitives;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.NestedGroupPortable;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.PrimitivePortable;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.PrimitivePortable.Init.FULL;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.PrimitivePortable.Init.NONE;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.PrimitivePortable.Init.NULL;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.TestPortableFactory;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.group;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.nested;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.prim;
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
@RunWith(Parameterized.class)
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
    // e.g. 'readInt', or generic 'read'
    private String readMethodNameToInvoke;
    // e.g. body.brain.iq
    private String pathToRead;
    // parent method of this test to identify it in case of failures
    private String parent;

    @Parameters(name = "{index}: {0}, read{2}, {3}")
    public static Collection<Object[]> parametrisationData() {
        List<Object[]> result = new ArrayList<>();

        directPrimitiveScenarios(result);
        directPrimitiveScenariosWrongMethodType(result);
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

    public DefaultPortableReaderSpecTest(Portable inputObject, Object expectedResult, Method method, String pathToRead,
                                         String parent) {
        this.inputObject = inputObject;
        this.expectedResult = expectedResult;
        this.readMethodNameToInvoke = method.name().replace("Generic", "");
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
        Object result = Invoker.invoke(reader(inputObject), readMethodNameToInvoke, pathToRead);
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
        desc += "method:\tread" + readMethodNameToInvoke + "\n";
        desc += "result:\t" + resultToMatch + "\n";
        desc += "input:\t" + inputObject + "\n";
        System.out.println(desc);
    }

    /**
     * Expands test cases for primitive non-array data types.
     * Word "primitive_" from the pathToExplode is replaced by each primitive type and the scenario is expanded to:
     * <ul>
     * <li>scenario(input, result.byte_, Byte, adjustedPath + "byte_"),</li>
     * <li>scenario(input, result.short_, Short, adjustedPath + "short_"),</li>
     * <li>scenario(input, result.int_, Int, adjustedPath + "int_"),</li>
     * <li>scenario(input, result.long_, Long, adjustedPath + "long_"),</li>
     * <li>scenario(input, result.float_, Float, adjustedPath + "float_"),</li>
     * <li>scenario(input, result.double_, Double, adjustedPath + "double_"),</li>
     * <li>scenario(input, result.boolean_, Boolean, adjustedPath + "boolean_"),</li>
     * <li>scenario(input, result.char_, Char, adjustedPath + "char_"),</li>
     * <li>scenario(input, result.string_, UTF, adjustedPath + "string_"),</li>
     * </ul>
     */
    private static Collection<Object[]> expandPrimitiveScenario(Portable input, Object result, String pathToExplode,
                                                                String parent) {
        List<Object[]> scenarios = new ArrayList<>();
        Object adjustedResult;
        String tokenToReplace = "primitive_";
        if (pathToExplode.contains("primitiveUTF_")) {
            tokenToReplace = "primitiveUTF_";
        }
        for (Method method : getPrimitives(tokenToReplace.contains("UTF"))) {
            if (result instanceof PrimitivePortable) {
                adjustedResult = ((PrimitivePortable) result).getPrimitive(method);
            } else if (result == null && method != UTF) {
                adjustedResult = IllegalArgumentException.class;
            } else {
                adjustedResult = result;
            }
            // type-specific method case
            scenarios.add(scenario(input, adjustedResult, method,
                    pathToExplode.replace(tokenToReplace, method.field), parent));
            // generic method case
            scenarios.add(scenario(input, adjustedResult == IllegalArgumentException.class ? null : adjustedResult, Generic,
                    pathToExplode.replace(tokenToReplace, method.field), parent));
        }
        return scenarios;
    }

    /**
     * Expands test cases for primitive non-array data types.
     * Word "primitive_" is replaced by each primitive type and the scenario is to call a wrong method type for the field.
     * <p>
     * So, for example, for primitive byte field we call methods for other data types.
     * <ul>
     * <li>scenario(input, result.byte_, Short, adjustedPath + "byte_"),</li>
     * <li>scenario(input, result.byte_, Int, adjustedPath + "byte_"),</li>
     * <li>scenario(input, result.byte_, Long, adjustedPath + "byte_"),</li>
     * <li>plus all other combinations that are incorrect (also with read*array method family)</li>
     * </ul>
     */
    @SuppressWarnings("SameParameterValue")
    private static Collection<Object[]> expandPrimitiveScenarioWrongMethodType(Portable input, String pathToExplode,
                                                                               String parent) {
        List<Object[]> scenarios = new ArrayList<>();
        String tokenToReplace = "primitive_";

        for (Method fieldMethod : getPrimitives(true)) {
            for (Method possibleMethod : getPrimitives(true)) {
                // type-specific method case
                String adjustedPath = pathToExplode.replace(tokenToReplace, fieldMethod.field);
                if (fieldMethod != possibleMethod) {
                    scenarios.add(scenario(input, IllegalArgumentException.class, possibleMethod, adjustedPath, parent));
                }
                scenarios.add(scenario(input, IllegalArgumentException.class, Method.getArrayMethodFor(possibleMethod),
                        adjustedPath, parent));
            }
        }
        return scenarios;
    }

    /**
     * Expands test cases for primitive array data types.
     * Word "primitive_" is replaced by each primitive type and the scenario is to call a wrong method type for the field.
     * <p>
     * So, for example, for primitive byte field we call methods for other data types.
     * <ul>
     * <li>scenario(input, result.bytes, Short, adjustedPath + "bytes"),</li>
     * <li>scenario(input, result.bytes, Int, adjustedPath + "bytes"),</li>
     * <li>scenario(input, result.bytes, Long, adjustedPath + "bytes"),</li>
     * <li>plus all other combinations that are incorrect (also with read*array method family)</li>
     * </ul>
     */
    @SuppressWarnings("SameParameterValue")
    private static Collection<Object[]> expandPrimitiveArrayScenarioWrongMethodType(Portable input, String pathToExplode, String parent) {
        List<Object[]> scenarios = new ArrayList<>();
        String tokenToReplace = "primitiveArray";

        for (Method fieldMethod : getPrimitiveArrays()) {
            for (Method possibleMethod : getPrimitives(true)) {
                // type-specific method case
                String adjustedPath = pathToExplode.replace(tokenToReplace, fieldMethod.field);
                if (fieldMethod != Method.getArrayMethodFor(possibleMethod)) {
                    scenarios.add(scenario(input, IllegalArgumentException.class, Method.getArrayMethodFor(possibleMethod),
                            adjustedPath, parent));
                }
                scenarios.add(scenario(input, IllegalArgumentException.class, possibleMethod, adjustedPath, parent));
            }
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
     * <li>for all primitives apart from UTF (exception expected)<ul>
     * <li>scenario(prim(NONE), IllegalArgumentException.class, Byte, "bytes[0]"),</li>
     * <li>scenario(prim(NULL), IllegalArgumentException.class, Byte, "bytes[1]"),</li>
     * </ul></li>
     * <p>
     * <li>for UTF (null expected)<ul>
     * <li>scenario(prim(NONE), null, UTF, "strings[0]"),</li>
     * <li>scenario(prim(NULL), null, UTF, "strings[1]"),</li>
     * </ul></li>
     * </ul>
     */
    private static Collection<Object[]> expandPrimitiveArrayScenario(Portable input, PrimitivePortable result,
                                                                     String pathToExplode, String parent) {
        List<Object[]> scenarios = new ArrayList<>();
        // group A:
        for (Method method : getPrimitiveArrays()) {
            String path = pathToExplode.replace("primitiveArray", method.field);
            Object resultToMatch = result != null ? result.getPrimitiveArray(method) : null;
            Object resultToMatchAny = resultToMatch;
            if (resultToMatchAny != null && Array.getLength(resultToMatchAny) == 0) {
                resultToMatchAny = null;
            }

            scenarios.addAll(asList(
                    scenario(input, resultToMatch, method, path, parent),
                    scenario(input, resultToMatchAny, method, path + "[any]", parent)
            ));
            scenarios.addAll(asList(
                    scenario(input, resultToMatch, Generic, path, parent),
                    scenario(input, resultToMatchAny, Generic, path + "[any]", parent)
            ));
        }

        // group B:
        for (Method method : getPrimitives(true)) {
            String path = pathToExplode.replace("primitiveArray", method.field).replace("_", "s");
            if (result == null
                    || result.getPrimitiveArray(method) == null
                    || Array.getLength(result.getPrimitiveArray(method)) == 0) {
                if (method.equals(UTF)) {
                    scenarios.addAll(asList(
                            scenario(input, null, method, path + "[0]", parent),
                            scenario(input, null, Generic, path + "[0]", parent))
                    );
                } else {
                    // IMPORTANT: the difference between generic and non-generic primitive call for null
                    scenarios.addAll(asList(
                            scenario(input, IllegalArgumentException.class, method, path + "[0]", parent),
                            scenario(input, null, Generic, path + "[0]", parent))
                    );
                }
            } else {
                scenarios.addAll(asList(
                        scenario(input, Array.get(result.getPrimitiveArray(method), 0), method, path + "[0]", parent),
                        scenario(input, Array.get(result.getPrimitiveArray(method), 1), method, path + "[1]", parent),
                        scenario(input, Array.get(result.getPrimitiveArray(method), 2), method, path + "[2]", parent)
                ));
                scenarios.addAll(asList(
                        scenario(input, Array.get(result.getPrimitiveArray(method), 0), Generic, path + "[0]", parent),
                        scenario(input, Array.get(result.getPrimitiveArray(method), 1), Generic, path + "[1]", parent),
                        scenario(input, Array.get(result.getPrimitiveArray(method), 2), Generic, path + "[2]", parent)
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
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private static Collection<Object[]> expandPortableArrayPrimitiveScenario(Portable input, GroupPortable result,
                                                                             String pathToExplode, String parent) {
        List<Object[]> scenarios = new ArrayList<>();
        // expansion of the portable array using the following quantifiers
        for (String token : asList("0", "1", "2", "any")) {

            String tokenToReplace = "primitive_";
            if (pathToExplode.contains("primitiveUTF_")) {
                tokenToReplace = "primitiveUTF_";
            }
            String path = pathToExplode.replace("portableArray", "portables[" + token + "]");
            if (token.equals("any")) {
                // B. case with [any] operator on portable array
                // expansion of the primitive fields
                for (Method method : getPrimitives(tokenToReplace.contains("UTF"))) {
                    List resultToMatch = new ArrayList();
                    int portableCount = 0;
                    try {
                        portableCount = result.portables.length;
                    } catch (NullPointerException ignored) {
                    }
                    for (int i = 0; i < portableCount; i++) {
                        PrimitivePortable portable = (PrimitivePortable) result.portables[i];
                        resultToMatch.add(portable.getPrimitive(method));
                    }
                    if (result == null || result.portables == null || result.portables.length == 0) {
                        resultToMatch = null;
                    }

                    scenarios.addAll(asList(
                            scenario(input, resultToMatch, getArrayMethodFor(method),
                                    path.replace(tokenToReplace, method.field), parent),
                            scenario(input, resultToMatch, Generic,
                                    path.replace(tokenToReplace, method.field), parent)
                    ));
                }
            } else {
                // A. case with [0], [1], [2] operator on portable array
                // expansion of the primitive fields
                for (Method method : getPrimitives(tokenToReplace.contains("UTF"))) {
                    Object resultToMatch = null;
                    try {
                        PrimitivePortable portable = (PrimitivePortable) result.portables[Integer.parseInt(token)];
                        resultToMatch = portable.getPrimitive(method);
                    } catch (NullPointerException ignored) {
                    } catch (IndexOutOfBoundsException ignored) {
                    }

                    if (method != UTF) {
                        if (result == null || result.portables == null || result.portables.length == 0) {
                            resultToMatch = IllegalArgumentException.class;
                        }
                    }

                    scenarios.addAll(asList(
                            scenario(input, resultToMatch, method, path.replace(tokenToReplace, method.field), parent),
                            // IMPORTANT: the difference between generic and non-generic primitive call for null
                            scenario(input, resultToMatch == IllegalArgumentException.class ? null : resultToMatch,
                                    Generic, path.replace(tokenToReplace, method.field), parent)
                    ));
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
        result.addAll(expandPrimitiveScenario(prim(FULL), prim(FULL), "primitiveUTF_", parent));

        // primitive arrays accessed directly (arrays are fully initialised, empty and null)
        result.addAll(expandPrimitiveArrayScenario(prim(FULL), prim(FULL), "primitiveArray", parent));
        result.addAll(expandPrimitiveArrayScenario(prim(NONE), prim(NONE), "primitiveArray", parent));
        result.addAll(expandPrimitiveArrayScenario(prim(NULL), prim(NULL), "primitiveArray", parent));
    }

    // ----------------------------------------------------------------------------------------------------------
    // DIRECT primitive and primitive-array access -> wrong method usage
    // ----------------------------------------------------------------------------------------------------------
    private static void directPrimitiveScenariosWrongMethodType(List<Object[]> result) {
        String parent = "directPrimitiveScenariosWrongMethodType";
        // FULLy initialised primitive objects accessed directly
        result.addAll(expandPrimitiveScenarioWrongMethodType(prim(FULL), "primitive_", parent));

        // primitive arrays accessed directly
        result.addAll(expandPrimitiveArrayScenarioWrongMethodType(prim(FULL), "primitiveArray", parent));
    }

    // ----------------------------------------------------------------------------------------------------------
    // from PORTABLE to primitive and primitive-array access
    // ----------------------------------------------------------------------------------------------------------
    private static void fromPortableToPrimitiveScenarios(List<Object[]> result) {
        String parent = "directPrimitiveScenariosWrongMethodType";
        // FULLy initialised primitive objects accessed from portable
        result.addAll(expandPrimitiveScenario(group(prim(FULL)), prim(FULL), "portable.primitiveUTF_", parent));

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
                "portableArray.primitiveUTF_", p));

        GroupPortable fullEmptyNullGroup = group(prim(1, FULL), prim(10, NONE), prim(100, NULL));
        result.addAll(expandPortableArrayPrimitiveScenario(fullEmptyNullGroup, fullEmptyNullGroup,
                "portableArray.primitiveUTF_", p));

        // empty or null portable array de-referenced further
        GroupPortable nullArrayGroup = new GroupPortable((Portable[]) null);
        result.addAll(expandPortableArrayPrimitiveScenario(nullArrayGroup, nullArrayGroup,
                "portableArray.primitiveUTF_", p));

        GroupPortable emptyArrayGroup = new GroupPortable(new Portable[0]);
        result.addAll(expandPortableArrayPrimitiveScenario(emptyArrayGroup, emptyArrayGroup,
                "portableArray.primitiveUTF_", p));

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
        result.addAll(expandPrimitiveScenario(emptyArrayGroup, IllegalArgumentException.class, "portables[0].primitive_", p));
        result.addAll(expandPrimitiveScenario(emptyArrayGroup, IllegalArgumentException.class, "portables[1].primitive_", p));
        result.addAll(expandPrimitiveScenario(emptyArrayGroup, IllegalArgumentException.class, "portables[2].primitive_", p));
        result.add(scenario(emptyArrayGroup, null, UTF, "portables[0].string_", p));
        result.add(scenario(emptyArrayGroup, null, UTF, "portables[1].string_", p));
        result.add(scenario(emptyArrayGroup, null, Generic, "portables[0].string_", p));
        result.add(scenario(emptyArrayGroup, null, Generic, "portables[1].string_", p));

        // EMPTY portable array -> de-referenced further for array access
        result.addAll(expandPrimitiveArrayScenario(emptyArrayGroup, null, "portables[0].primitiveArray", p));
        result.addAll(expandPrimitiveArrayScenario(emptyArrayGroup, null, "portables[1].primitiveArray", p));
        result.addAll(expandPrimitiveArrayScenario(emptyArrayGroup, null, "portables[2].primitiveArray", p));

        // NULL portable array -> de-referenced further for primitive access
        result.addAll(expandPrimitiveScenario(nullArrayGroup, IllegalArgumentException.class, "portables[0].primitive_", p));
        result.addAll(expandPrimitiveScenario(nullArrayGroup, IllegalArgumentException.class, "portables[1].primitive_", p));
        result.addAll(expandPrimitiveScenario(nullArrayGroup, IllegalArgumentException.class, "portables[2].primitive_", p));
        result.add(scenario(nullArrayGroup, null, UTF, "portables[0].string_", p));
        result.add(scenario(nullArrayGroup, null, UTF, "portables[1].string_", p));
        result.add(scenario(nullArrayGroup, null, Generic, "portables[0].string_", p));
        result.add(scenario(nullArrayGroup, null, Generic, "portables[1].string_", p));

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
                scenario(nestedFullGroup, (nestedFullGroup.portable), Portable,
                        "portable", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portable)).portable, Portable,
                        "portable.portable", p)
        ));
        result.addAll(asList(
                scenario(nestedFullGroup, (nestedFullGroup.portable), Generic,
                        "portable", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portable)).portable, Generic,
                        "portable.portable", p)
        ));
        result.addAll(expandPrimitiveScenario(nestedFullGroup, ((GroupPortable) nestedFullGroup.portable).portable,
                "portable.portable.primitiveUTF_", p));
        result.addAll(expandPrimitiveArrayScenario(nestedFullGroup,
                (PrimitivePortable) ((GroupPortable) nestedFullGroup.portable).portable,
                "portable.portable.primitiveArray", p));

        NestedGroupPortable nestedFullEmptyNullGroup = nested(group(prim(1, FULL), prim(10, NONE), prim(100, NULL)));
        result.addAll(expandPrimitiveScenario(nestedFullEmptyNullGroup,
                ((GroupPortable) nestedFullEmptyNullGroup.portable).portable,
                "portable.portable.primitiveUTF_", p));
        result.addAll(expandPrimitiveArrayScenario(nestedFullEmptyNullGroup,
                (PrimitivePortable) ((GroupPortable) nestedFullEmptyNullGroup.portable).portable,
                "portable.portable.primitiveArray", p));

        // empty or null portable array de-referenced further
        NestedGroupPortable nestedNullArrayGroup = nested(new GroupPortable((Portable[]) null));
        result.addAll(asList(
                scenario(nestedNullArrayGroup, (nestedNullArrayGroup.portable), Portable, "portable", p),
                scenario(nestedNullArrayGroup, null, Portable, "portable.portable", p)
        ));
        result.addAll(asList(
                scenario(nestedNullArrayGroup, (nestedNullArrayGroup.portable), Generic, "portable", p),
                scenario(nestedNullArrayGroup, null, Generic, "portable.portable", p)
        ));
        result.addAll(expandPrimitiveScenario(nestedNullArrayGroup, null, "portable.portable.primitiveUTF_", p));
        result.addAll(expandPrimitiveArrayScenario(nestedNullArrayGroup, null, "portable.portable.primitiveArray", p));


        NestedGroupPortable nestedNull = nested(new Portable[0]);
        result.addAll(asList(
                scenario(nestedNull, null, Portable, "portable", p),
                scenario(nestedNull, null, Portable, "portable.portable", p)
        ));
        result.addAll(asList(
                scenario(nestedNull, null, Generic, "portable", p),
                scenario(nestedNull, null, Generic, "portable.portable", p)
        ));
        result.addAll(expandPrimitiveScenario(nestedNull, null, "portable.portable.primitiveUTF_", p));
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
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portable)).portables, PortableArray,
                        "portable.portables", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portable)).portables, PortableArray,
                        "portable.portables[any]", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portable)).portables[0], Portable,
                        "portable.portables[0]", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portable)).portables[1], Portable,
                        "portable.portables[1]", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portable)).portables[2], Portable,
                        "portable.portables[2]", p),
                scenario(nestedFullGroup, null, Portable, "portable.portables[12]", p)
        ));
        result.addAll(asList(
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portable)).portables, Generic,
                        "portable.portables", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portable)).portables, Generic,
                        "portable.portables[any]", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portable)).portables[0], Generic,
                        "portable.portables[0]", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portable)).portables[1], Generic,
                        "portable.portables[1]", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portable)).portables[2], Generic,
                        "portable.portables[2]", p),
                scenario(nestedFullGroup, null, Generic, "portable.portables[12]", p)
        ));
        result.addAll(expandPortableArrayPrimitiveScenario(nestedFullGroup, (GroupPortable) nestedFullGroup.portable,
                "portable.portableArray.primitiveUTF_", p)
        );


        NestedGroupPortable nestedFullEmptyNullGroup = nested(group(prim(1, FULL), prim(10, NONE), prim(100, NULL)));
        result.addAll(expandPortableArrayPrimitiveScenario(nestedFullEmptyNullGroup,
                (GroupPortable) nestedFullEmptyNullGroup.portable, "portable.portableArray.primitiveUTF_", p)
        );

        // empty or null portable array de-referenced further
        NestedGroupPortable nestedNullArrayGroup = nested(new GroupPortable((Portable[]) null));
        result.addAll(asList(
                scenario(nestedNullArrayGroup, null, PortableArray, "portable.portables", p),
                scenario(nestedNullArrayGroup, null, PortableArray, "portable.portables[any]", p),
                scenario(nestedNullArrayGroup, null, Portable, "portable.portables[0]", p),
                scenario(nestedNullArrayGroup, null, Portable, "portable.portables[1]", p),
                scenario(nestedNullArrayGroup, null, Portable, "portable.portables[2]", p)
        ));
        result.addAll(asList(
                scenario(nestedNullArrayGroup, null, Generic, "portable.portables", p),
                scenario(nestedNullArrayGroup, null, Generic, "portable.portables[any]", p),
                scenario(nestedNullArrayGroup, null, Generic, "portable.portables[0]", p),
                scenario(nestedNullArrayGroup, null, Generic, "portable.portables[1]", p),
                scenario(nestedNullArrayGroup, null, Generic, "portable.portables[2]", p)
        ));
        result.addAll(expandPortableArrayPrimitiveScenario(nestedNullArrayGroup, (GroupPortable) nestedNullArrayGroup.portable,
                "portable.portableArray.primitiveUTF_", p)
        );

        NestedGroupPortable nestedEmptyArrayGroup = nested(new GroupPortable(new Portable[0]));
        result.addAll(asList(
                scenario(nestedEmptyArrayGroup, new Portable[0], PortableArray, "portable.portables", p),
                scenario(nestedEmptyArrayGroup, null, PortableArray, "portable.portables[any]", p),
                scenario(nestedEmptyArrayGroup, null, Portable, "portable.portables[0]", p),
                scenario(nestedEmptyArrayGroup, null, Portable, "portable.portables[1]", p),
                scenario(nestedEmptyArrayGroup, null, Portable, "portable.portables[2]", p)
        ));
        result.addAll(asList(
                scenario(nestedEmptyArrayGroup, new Portable[0], Generic, "portable.portables", p),
                scenario(nestedEmptyArrayGroup, null, Generic, "portable.portables[any]", p),
                scenario(nestedEmptyArrayGroup, null, Generic, "portable.portables[0]", p),
                scenario(nestedEmptyArrayGroup, null, Generic, "portable.portables[1]", p),
                scenario(nestedEmptyArrayGroup, null, Generic, "portable.portables[2]", p)
        ));
        result.addAll(expandPortableArrayPrimitiveScenario(nestedEmptyArrayGroup,
                (GroupPortable) nestedEmptyArrayGroup.portable, "portable.portableArray.primitiveUTF_", p)
        );

        NestedGroupPortable nestedEmpty = nested(new GroupPortable[0]);
        result.addAll(asList(
                scenario(nestedEmpty, null, PortableArray, "portable.portables", p),
                scenario(nestedEmpty, null, PortableArray, "portable.portables[any]", p),
                scenario(nestedEmpty, null, Portable, "portable.portables[0]", p),
                scenario(nestedEmpty, null, Portable, "portable.portables[1]", p),
                scenario(nestedEmpty, null, Portable, "portable.portables[2]", p)
        ));
        result.addAll(asList(
                scenario(nestedEmpty, null, Generic, "portable.portables", p),
                scenario(nestedEmpty, null, Generic, "portable.portables[any]", p),
                scenario(nestedEmpty, null, Generic, "portable.portables[0]", p),
                scenario(nestedEmpty, null, Generic, "portable.portables[1]", p),
                scenario(nestedEmpty, null, Generic, "portable.portables[2]", p)
        ));
        result.addAll(expandPortableArrayPrimitiveScenario(nestedEmpty, (GroupPortable) nestedEmpty.portable,
                "portable.portableArray.primitiveUTF_", p)
        );

        NestedGroupPortable nestedNull = nested((GroupPortable[]) null);
        result.addAll(asList(
                scenario(nestedNull, null, PortableArray, "portable.portables", p),
                scenario(nestedNull, null, PortableArray, "portable.portables[any]", p),
                scenario(nestedNull, null, Portable, "portable.portables[0]", p),
                scenario(nestedNull, null, Portable, "portable.portables[1]", p),
                scenario(nestedNull, null, Portable, "portable.portables[2]", p)
        ));
        result.addAll(asList(
                scenario(nestedNull, null, Generic, "portable.portables", p),
                scenario(nestedNull, null, Generic, "portable.portables[any]", p),
                scenario(nestedNull, null, Generic, "portable.portables[0]", p),
                scenario(nestedNull, null, Generic, "portable.portables[1]", p),
                scenario(nestedNull, null, Generic, "portable.portables[2]", p)
        ));
        result.addAll(expandPortableArrayPrimitiveScenario(nestedNull, (GroupPortable) nestedNull.portable,
                "portable.portableArray.primitiveUTF_", p)
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

        Class failure = IllegalArgumentException.class;
        result.addAll(asList(
                scenario(input, failure, ByteArray, "portables[any].portables[any].byte_", p),
                scenario(input, failure, ShortArray, "portables[any].portables[any].short_", p),
                scenario(input, failure, IntArray, "portables[any].portables[any].int_", p),
                scenario(input, failure, LongArray, "portables[any].portables[any].long_", p),
                scenario(input, failure, CharArray, "portables[any].portables[any].char_", p),
                scenario(input, failure, FloatArray, "portables[any].portables[any].float_", p),
                scenario(input, failure, DoubleArray, "portables[any].portables[any].double_", p),
                scenario(input, failure, BooleanArray, "portables[any].portables[any].boolean_", p)
        ));
        List expectedUtfArray = list(null, p1.string_, p10.string_, null, p20.string_);
        result.add(scenario(input, expectedUtfArray, UTFArray, "portables[any].portables[any].string_", p));

        result.addAll(asList(
                scenario(input, list(null, p1.byte_, p10.byte_, p20.byte_), Generic,
                        "portables[any].portables[any].byte_", p),
                scenario(input, list(null, p1.short_, p10.short_, p20.short_), Generic,
                        "portables[any].portables[any].short_", p),
                scenario(input, list(null, p1.int_, p10.int_, p20.int_), Generic,
                        "portables[any].portables[any].int_", p),
                scenario(input, list(null, p1.long_, p10.long_, p20.long_), Generic,
                        "portables[any].portables[any].long_", p),
                scenario(input, list(null, p1.char_, p10.char_, p20.char_), Generic,
                        "portables[any].portables[any].char_", p),
                scenario(input, list(null, p1.float_, p10.float_, p20.float_), Generic,
                        "portables[any].portables[any].float_", p),
                scenario(input, list(null, p1.double_, p10.double_, p20.double_), Generic,
                        "portables[any].portables[any].double_", p),
                scenario(input, list(null, p1.boolean_, p10.boolean_, p20.boolean_), Generic,
                        "portables[any].portables[any].boolean_", p),
                scenario(input, list(null, p1.string_, p10.string_, p20.string_), Generic,
                        "portables[any].portables[any].string_", p)
        ));

        // =============================================
        // INPUT empty
        // =============================================
        NestedGroupPortable inputEmpty = nested(
                new Portable[0]
        );
        result.addAll(asList(
                scenario(inputEmpty, null, ByteArray, "portables[any].portables[any].byte_", p),
                scenario(inputEmpty, null, ShortArray, "portables[any].portables[any].short_", p),
                scenario(inputEmpty, null, IntArray, "portables[any].portables[any].int_", p),
                scenario(inputEmpty, null, LongArray, "portables[any].portables[any].long_", p),
                scenario(inputEmpty, null, CharArray, "portables[any].portables[any].char_", p),
                scenario(inputEmpty, null, FloatArray, "portables[any].portables[any].float_", p),
                scenario(inputEmpty, null, DoubleArray, "portables[any].portables[any].double_", p),
                scenario(inputEmpty, null, BooleanArray, "portables[any].portables[any].boolean_", p),
                scenario(inputEmpty, null, UTFArray, "portables[any].portables[any].string_", p)
        ));

        result.addAll(asList(
                scenario(inputEmpty, null, Generic, "portables[any].portables[any].byte_", p),
                scenario(inputEmpty, null, Generic, "portables[any].portables[any].short_", p),
                scenario(inputEmpty, null, Generic, "portables[any].portables[any].int_", p),
                scenario(inputEmpty, null, Generic, "portables[any].portables[any].long_", p),
                scenario(inputEmpty, null, Generic, "portables[any].portables[any].char_", p),
                scenario(inputEmpty, null, Generic, "portables[any].portables[any].float_", p),
                scenario(inputEmpty, null, Generic, "portables[any].portables[any].double_", p),
                scenario(inputEmpty, null, Generic, "portables[any].portables[any].boolean_", p),
                scenario(inputEmpty, null, Generic, "portables[any].portables[any].string_", p)
        ));

        // =============================================
        // INPUT null
        // =============================================
        NestedGroupPortable inputNull = nested((Portable[]) null);
        result.addAll(asList(
                scenario(inputNull, null, ByteArray, "portables[any].portables[any].byte_", p),
                scenario(inputNull, null, ShortArray, "portables[any].portables[any].short_", p),
                scenario(inputNull, null, IntArray, "portables[any].portables[any].int_", p),
                scenario(inputNull, null, LongArray, "portables[any].portables[any].long_", p),
                scenario(inputNull, null, CharArray, "portables[any].portables[any].char_", p),
                scenario(inputNull, null, FloatArray, "portables[any].portables[any].float_", p),
                scenario(inputNull, null, DoubleArray, "portables[any].portables[any].double_", p),
                scenario(inputNull, null, BooleanArray, "portables[any].portables[any].boolean_", p),
                scenario(inputNull, null, UTFArray, "portables[any].portables[any].string_", p)
        ));

        result.addAll(asList(
                scenario(inputNull, null, Generic, "portables[any].portables[any].byte_", p),
                scenario(inputNull, null, Generic, "portables[any].portables[any].short_", p),
                scenario(inputNull, null, Generic, "portables[any].portables[any].int_", p),
                scenario(inputNull, null, Generic, "portables[any].portables[any].long_", p),
                scenario(inputNull, null, Generic, "portables[any].portables[any].char_", p),
                scenario(inputNull, null, Generic, "portables[any].portables[any].float_", p),
                scenario(inputNull, null, Generic, "portables[any].portables[any].double_", p),
                scenario(inputNull, null, Generic, "portables[any].portables[any].boolean_", p),
                scenario(inputNull, null, Generic, "portables[any].portables[any].string_", p)
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
        Class failure = IllegalArgumentException.class;
        result.addAll(asList(
                scenario(input, failure, ByteArray, "portables[any].portables[any].bytes[any]", p),
                scenario(input, failure, ShortArray, "portables[any].portables[any].shorts[any]", p),
                scenario(input, failure, IntArray, "portables[any].portables[any].ints[any]", p),
                scenario(input, failure, LongArray, "portables[any].portables[any].longs[any]", p),
                scenario(input, failure, CharArray, "portables[any].portables[any].chars[any]", p),
                scenario(input, failure, FloatArray, "portables[any].portables[any].floats[any]", p),
                scenario(input, failure, DoubleArray, "portables[any].portables[any].doubles[any]", p),
                scenario(input, failure, BooleanArray, "portables[any].portables[any].booleans[any]", p)
        ));
        List expectedUtfArray = list(null, null, p10.strings, null, null, p20.strings, null);
        result.add(scenario(input, expectedUtfArray, UTFArray, "portables[any].portables[any].strings[any]", p));

        result.addAll(asList(
                scenario(input, list(null, p10.bytes, p20.bytes), Generic,
                        "portables[any].portables[any].bytes[any]", p),
                scenario(input, list(null, p10.shorts, p20.shorts), Generic,
                        "portables[any].portables[any].shorts[any]", p),
                scenario(input, list(null, p10.ints, p20.ints), Generic,
                        "portables[any].portables[any].ints[any]", p),
                scenario(input, list(null, p10.longs, p20.longs), Generic,
                        "portables[any].portables[any].longs[any]", p),
                scenario(input, list(null, p10.chars, p20.chars), Generic,
                        "portables[any].portables[any].chars[any]", p),
                scenario(input, list(null, p10.floats, p20.floats), Generic,
                        "portables[any].portables[any].floats[any]", p),
                scenario(input, list(null, p10.doubles, p20.doubles), Generic,
                        "portables[any].portables[any].doubles[any]", p),
                scenario(input, list(null, p10.booleans, p20.booleans), Generic,
                        "portables[any].portables[any].booleans[any]", p),
                scenario(input, list(null, p10.strings, p20.strings), Generic,
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
                scenario(inputEmpty, null, ByteArray, "portables[any].portables[any].bytes[any]", p),
                scenario(inputEmpty, null, ShortArray, "portables[any].portables[any].shorts[any]", p),
                scenario(inputEmpty, null, IntArray, "portables[any].portables[any].ints[any]", p),
                scenario(inputEmpty, null, LongArray, "portables[any].portables[any].longs[any]", p),
                scenario(inputEmpty, null, CharArray, "portables[any].portables[any].chars[any]", p),
                scenario(inputEmpty, null, FloatArray, "portables[any].portables[any].floats[any]", p),
                scenario(inputEmpty, null, DoubleArray, "portables[any].portables[any].doubles[any]", p),
                scenario(inputEmpty, null, BooleanArray, "portables[any].portables[any].booleans[any]", p),
                scenario(inputEmpty, null, UTFArray, "portables[any].portables[any].strings[any]", p)
        ));

        result.addAll(asList(
                scenario(inputEmpty, null, Generic, "portables[any].portables[any].bytes[any]", p),
                scenario(inputEmpty, null, Generic, "portables[any].portables[any].shorts[any]", p),
                scenario(inputEmpty, null, Generic, "portables[any].portables[any].ints[any]", p),
                scenario(inputEmpty, null, Generic, "portables[any].portables[any].longs[any]", p),
                scenario(inputEmpty, null, Generic, "portables[any].portables[any].chars[any]", p),
                scenario(inputEmpty, null, Generic, "portables[any].portables[any].floats[any]", p),
                scenario(inputEmpty, null, Generic, "portables[any].portables[any].doubles[any]", p),
                scenario(inputEmpty, null, Generic, "portables[any].portables[any].booleans[any]", p),
                scenario(inputEmpty, null, Generic, "portables[any].portables[any].strings[any]", p)
        ));

        // =============================================
        // INPUT null
        // =============================================
        p = method + " null";
        NestedGroupPortable inputNull = nested((Portable[]) null);
        result.addAll(asList(
                scenario(inputNull, null, ByteArray, "portables[any].portables[any].bytes[any]", p),
                scenario(inputNull, null, ShortArray, "portables[any].portables[any].shorts[any]", p),
                scenario(inputNull, null, IntArray, "portables[any].portables[any].ints[any]", p),
                scenario(inputNull, null, LongArray, "portables[any].portables[any].longs[any]", p),
                scenario(inputNull, null, CharArray, "portables[any].portables[any].chars[any]", p),
                scenario(inputNull, null, FloatArray, "portables[any].portables[any].floats[any]", p),
                scenario(inputNull, null, DoubleArray, "portables[any].portables[any].doubles[any]", p),
                scenario(inputNull, null, BooleanArray, "portables[any].portables[any].booleans[any]", p),
                scenario(inputNull, null, UTFArray, "portables[any].portables[any].strings[any]", p)
        ));

        result.addAll(asList(
                scenario(inputNull, null, Generic, "portables[any].portables[any].bytes[any]", p),
                scenario(inputNull, null, Generic, "portables[any].portables[any].shorts[any]", p),
                scenario(inputNull, null, Generic, "portables[any].portables[any].ints[any]", p),
                scenario(inputNull, null, Generic, "portables[any].portables[any].longs[any]", p),
                scenario(inputNull, null, Generic, "portables[any].portables[any].chars[any]", p),
                scenario(inputNull, null, Generic, "portables[any].portables[any].floats[any]", p),
                scenario(inputNull, null, Generic, "portables[any].portables[any].doubles[any]", p),
                scenario(inputNull, null, Generic, "portables[any].portables[any].booleans[any]", p),
                scenario(inputNull, null, Generic, "portables[any].portables[any].strings[any]", p)
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
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portables[0])).portables, PortableArray,
                        "portables[0].portables", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portables[0])).portables, PortableArray,
                        "portables[0].portables[any]", p),
                scenario(nestedFullGroup, new Portable[]{prim(1, FULL)}, PortableArray,
                        "portables[any].portables[0]", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portables[0])).portables, PortableArray,
                        "portables[any].portables[any]", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portables[0])).portables[0], Portable,
                        "portables[0].portables[0]", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portables[0])).portables[1], Portable,
                        "portables[0].portables[1]", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portables[0])).portables[2], Portable,
                        "portables[0].portables[2]", p),
                scenario(nestedFullGroup, null, Portable,
                        "portables[0].portables[12]", p)
        ));
        result.addAll(asList(
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portables[0])).portables, Generic,
                        "portables[0].portables", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portables[0])).portables, Generic,
                        "portables[0].portables[any]", p),
                scenario(nestedFullGroup, new Portable[]{prim(1, FULL)}, Generic,
                        "portables[any].portables[0]", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portables[0])).portables, Generic,
                        "portables[any].portables[any]", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portables[0])).portables[0], Generic,
                        "portables[0].portables[0]", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portables[0])).portables[1], Generic,
                        "portables[0].portables[1]", p),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portables[0])).portables[2], Generic,
                        "portables[0].portables[2]", p),
                scenario(nestedFullGroup, null, Generic,
                        "portables[0].portables[12]", p)
        ));

        result.addAll(expandPortableArrayPrimitiveScenario(nestedFullGroup, (GroupPortable) nestedFullGroup.portable,
                "portables[0].portableArray.primitiveUTF_", p)
        );

        NestedGroupPortable anyGroup = nested(new Portable[]{
                group(prim(1, FULL), prim(10, NONE), prim(50, NULL)), group(prim(2, FULL), prim(20, NONE), prim(80, NULL)),
        });
        result.addAll(expandPortableArrayPrimitiveScenario(anyGroup, (GroupPortable) anyGroup.portables[0],
                "portables[0].portableArray.primitiveUTF_", p)
        );

        // empty or null portable array de-referenced further
        NestedGroupPortable nestedNullArrayGroup = nested(new GroupPortable((Portable[]) null));
        result.addAll(asList(
                scenario(nestedNullArrayGroup, null, PortableArray, "portables[0].portables", p),
                scenario(nestedNullArrayGroup, null, PortableArray, "portables[0].portables[any]", p),
                scenario(nestedNullArrayGroup, null, PortableArray, "portables[any].portables[0]", p),
                scenario(nestedNullArrayGroup, null, PortableArray, "portables[any].portables[any]", p),
                scenario(nestedNullArrayGroup, null, Portable, "portables[0].portables[0]", p),
                scenario(nestedNullArrayGroup, null, Portable, "portables[0].portables[1]", p),
                scenario(nestedNullArrayGroup, null, Portable, "portables[0].portables[2]", p)
        ));
        result.addAll(asList(
                scenario(nestedNullArrayGroup, null, Generic, "portables[0].portables", p),
                scenario(nestedNullArrayGroup, null, Generic, "portables[0].portables[any]", p),
                scenario(nestedNullArrayGroup, null, Generic, "portables[any].portables[0]", p),
                scenario(nestedNullArrayGroup, null, Generic, "portables[any].portables[any]", p),
                scenario(nestedNullArrayGroup, null, Generic, "portables[0].portables[0]", p),
                scenario(nestedNullArrayGroup, null, Generic, "portables[0].portables[1]", p),
                scenario(nestedNullArrayGroup, null, Generic, "portables[0].portables[2]", p)
        ));
        result.addAll(expandPortableArrayPrimitiveScenario(nestedNullArrayGroup,
                (GroupPortable) nestedNullArrayGroup.portable, "portables[0].portableArray.primitiveUTF_", p)
        );

        NestedGroupPortable nestedEmptyArrayGroup = nested(new GroupPortable(new Portable[0]));
        result.addAll(asList(
                scenario(nestedEmptyArrayGroup, new Portable[0], PortableArray, "portables[0].portables", p),
                scenario(nestedEmptyArrayGroup, null, PortableArray, "portables[0].portables[any]", p),
                scenario(nestedEmptyArrayGroup, null, PortableArray, "portables[any].portables[0]", p),
                scenario(nestedEmptyArrayGroup, null, PortableArray, "portables[any].portables[any]", p),
                scenario(nestedEmptyArrayGroup, null, Portable, "portables[0].portables[0]", p),
                scenario(nestedEmptyArrayGroup, null, Portable, "portables[0].portables[1]", p),
                scenario(nestedEmptyArrayGroup, null, Portable, "portables[0].portables[2]", p)
        ));
        result.addAll(asList(
                scenario(nestedEmptyArrayGroup, new Portable[0], Generic, "portables[0].portables", p),
                scenario(nestedEmptyArrayGroup, null, Generic, "portables[0].portables[any]", p),
                scenario(nestedEmptyArrayGroup, null, Generic, "portables[any].portables[0]", p),
                scenario(nestedEmptyArrayGroup, null, Generic, "portables[any].portables[any]", p),
                scenario(nestedEmptyArrayGroup, null, Generic, "portables[0].portables[0]", p),
                scenario(nestedEmptyArrayGroup, null, Generic, "portables[0].portables[1]", p),
                scenario(nestedEmptyArrayGroup, null, Generic, "portables[0].portables[2]", p)
        ));
        result.addAll(expandPortableArrayPrimitiveScenario(nestedEmptyArrayGroup,
                (GroupPortable) nestedEmptyArrayGroup.portable, "portables[0].portableArray.primitiveUTF_", p)
        );

        NestedGroupPortable nestedEmpty = nested(new GroupPortable[0]);
        result.addAll(asList(
                scenario(nestedEmpty, null, PortableArray, "portables[0].portables", p),
                scenario(nestedEmpty, null, PortableArray, "portables[0].portables[any]", p),
                scenario(nestedEmpty, null, PortableArray, "portables[any].portables[0]", p),
                scenario(nestedEmpty, null, PortableArray, "portables[any].portables[any]", p),
                scenario(nestedEmpty, null, Portable, "portables[0].portables[0]", p),
                scenario(nestedEmpty, null, Portable, "portables[0].portables[1]", p),
                scenario(nestedEmpty, null, Portable, "portables[0].portables[2]", p)
        ));
        result.addAll(asList(
                scenario(nestedEmpty, null, Generic, "portables[0].portables", p),
                scenario(nestedEmpty, null, Generic, "portables[0].portables[any]", p),
                scenario(nestedEmpty, null, Generic, "portables[any].portables[0]", p),
                scenario(nestedEmpty, null, Generic, "portables[any].portables[any]", p),
                scenario(nestedEmpty, null, Generic, "portables[0].portables[0]", p),
                scenario(nestedEmpty, null, Generic, "portables[0].portables[1]", p),
                scenario(nestedEmpty, null, Generic, "portables[0].portables[2]", p)
        ));
        result.addAll(expandPortableArrayPrimitiveScenario(nestedEmpty,
                (GroupPortable) nestedEmpty.portable, "portables[0].portableArray.primitiveUTF_", p)
        );

        NestedGroupPortable nestedNull = nested((GroupPortable[]) null);
        result.addAll(asList(
                scenario(nestedNull, null, PortableArray, "portables[0].portables", p),
                scenario(nestedNull, null, PortableArray, "portables[0].portables[any]", p),
                scenario(nestedNull, null, PortableArray, "portables[any].portables[0]", p),
                scenario(nestedNull, null, PortableArray, "portables[any].portables[any]", p),
                scenario(nestedNull, null, Portable, "portables[0].portables[0]", p),
                scenario(nestedNull, null, Portable, "portables[0].portables[1]", p),
                scenario(nestedNull, null, Portable, "portables[0].portables[2]", p)
        ));
        result.addAll(asList(
                scenario(nestedNull, null, Generic, "portables[0].portables", p),
                scenario(nestedNull, null, Generic, "portables[0].portables[any]", p),
                scenario(nestedNull, null, Generic, "portables[any].portables[0]", p),
                scenario(nestedNull, null, Generic, "portables[any].portables[any]", p),
                scenario(nestedNull, null, Generic, "portables[0].portables[0]", p),
                scenario(nestedNull, null, Generic, "portables[0].portables[1]", p),
                scenario(nestedNull, null, Generic, "portables[0].portables[2]", p)
        ));
        result.addAll(expandPortableArrayPrimitiveScenario(nestedNull,
                (GroupPortable) nestedNull.portable, "portables[0].portableArray.primitiveUTF_", p)
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
                scenario(anyGroup, ((GroupPortable) (anyGroup.portables[0])).portables, PortableArray,
                        "portables[0].portables[any]", p),
                scenario(anyGroup, new Portable[]{prim(1, FULL), prim(2, FULL)}, PortableArray,
                        "portables[any].portables[0]", p),
                scenario(anyGroup, new Portable[]{prim(10, FULL), prim(20, FULL)}, PortableArray,
                        "portables[any].portables[1]", p),
                scenario(anyGroup, new Portable[]{prim(50, FULL), prim(80, FULL)}, PortableArray,
                        "portables[any].portables[2]", p),
                scenario(anyGroup, new Portable[]{prim(1, FULL), prim(10, FULL), prim(50, FULL), prim(2, FULL), prim(20, FULL),
                        prim(80, FULL),
                }, PortableArray, "portables[any].portables[any]", p)
        ));
        result.addAll(asList(
                scenario(anyGroup, ((GroupPortable) (anyGroup.portables[0])).portables, Generic,
                        "portables[0].portables[any]", p),
                scenario(anyGroup, new Portable[]{prim(1, FULL), prim(2, FULL)}, Generic,
                        "portables[any].portables[0]", p),
                scenario(anyGroup, new Portable[]{prim(10, FULL), prim(20, FULL)}, Generic,
                        "portables[any].portables[1]", p),
                scenario(anyGroup, new Portable[]{prim(50, FULL), prim(80, FULL)}, Generic,
                        "portables[any].portables[2]", p),
                scenario(anyGroup, new Portable[]{prim(1, FULL), prim(10, FULL), prim(50, FULL), prim(2, FULL), prim(20, FULL),
                        prim(80, FULL),
                }, Generic, "portables[any].portables[any]", p)
        ));

        NestedGroupPortable nestedEmptyArrayGroup = nested(new Portable[]{new GroupPortable(new Portable[0]),
                group(prim(1, FULL), prim(10, NONE), prim(50, NULL)),
        });
        result.addAll(asList(
                scenario(nestedEmptyArrayGroup, null, PortableArray,
                        "portables[0].portables[any]", p),
                scenario(nestedEmptyArrayGroup, new Portable[]{null, prim(1, FULL)}, PortableArray,
                        "portables[any].portables[0]", p),
                scenario(nestedEmptyArrayGroup, new Portable[]{null, prim(10, FULL)}, PortableArray,
                        "portables[any].portables[1]", p),
                scenario(nestedEmptyArrayGroup, new Portable[]{null, prim(50, FULL)}, PortableArray,
                        "portables[any].portables[2]", p),
                scenario(nestedEmptyArrayGroup, new Portable[]{null, prim(1, FULL), prim(10, FULL), prim(50, FULL)},
                        PortableArray, "portables[any].portables[any]", p)
        ));
        result.addAll(asList(
                scenario(nestedEmptyArrayGroup, null, Generic,
                        "portables[0].portables[any]", p),
                scenario(nestedEmptyArrayGroup, new Portable[]{null, prim(1, FULL)}, Generic,
                        "portables[any].portables[0]", p),
                scenario(nestedEmptyArrayGroup, new Portable[]{null, prim(10, FULL)}, Generic,
                        "portables[any].portables[1]", p),
                scenario(nestedEmptyArrayGroup, new Portable[]{null, prim(50, FULL)}, Generic,
                        "portables[any].portables[2]", p),
                scenario(nestedEmptyArrayGroup, new Portable[]{null, prim(1, FULL), prim(10, FULL), prim(50, FULL)}, Generic,
                        "portables[any].portables[any]", p)
        ));

        NestedGroupPortable nestedNullArrayGroup = nested(new Portable[]{new GroupPortable((Portable[]) null),
                group(prim(1, FULL), prim(10, NONE), prim(50, NULL)),
        });
        result.addAll(asList(
                scenario(nestedNullArrayGroup, null, PortableArray,
                        "portables[0].portables[any]", p),
                scenario(nestedNullArrayGroup, new Portable[]{null, prim(1, FULL)}, PortableArray,
                        "portables[any].portables[0]", p),
                scenario(nestedNullArrayGroup, new Portable[]{null, prim(10, FULL)}, PortableArray,
                        "portables[any].portables[1]", p),
                scenario(nestedNullArrayGroup, new Portable[]{null, prim(50, FULL)}, PortableArray,
                        "portables[any].portables[2]", p),
                scenario(nestedNullArrayGroup, new Portable[]{null, prim(1, FULL), prim(10, FULL), prim(50, FULL)},
                        PortableArray, "portables[any].portables[any]", p)
        ));
        result.addAll(asList(
                scenario(nestedNullArrayGroup, null, PortableArray,
                        "portables[0].portables[any]", p),
                scenario(nestedNullArrayGroup, new Portable[]{null, prim(1, FULL)}, Generic,
                        "portables[any].portables[0]", p),
                scenario(nestedNullArrayGroup, new Portable[]{null, prim(10, FULL)}, Generic,
                        "portables[any].portables[1]", p),
                scenario(nestedNullArrayGroup, new Portable[]{null, prim(50, FULL)}, Generic,
                        "portables[any].portables[2]", p),
                scenario(nestedNullArrayGroup, new Portable[]{null, prim(1, FULL), prim(10, FULL), prim(50, FULL)}, Generic,
                        "portables[any].portables[any]", p)
        ));
    }

    //
    // Test data structure utilities
    //

    private static Object[] scenario(Portable input, Object result, Method method, String path, String parent) {
        return new Object[]{input, result, method, path, parent};
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

    public PortableReader reader(Portable portable) throws IOException {
        Config config = new Config();
        config.getSerializationConfig().addPortableFactory(TestPortableFactory.ID,
                new TestPortableFactory());

        HazelcastInstanceProxy hz = (HazelcastInstanceProxy) createHazelcastInstance(config);
        IMap<String, Object> map = hz.getMap("stealingMap");

        // put fully initialised object to a map
        // avoid the case where there's no class definition
        if (portable instanceof PrimitivePortable) {
            map.put(P_NON_EMPTY.toString(), P_NON_EMPTY);
        }
        if (portable instanceof GroupPortable) {
            map.put(G_NON_EMPTY.toString(), G_NON_EMPTY);
        }
        if (portable instanceof NestedGroupPortable) {
            map.put(N_NON_EMPTY.toString(), N_NON_EMPTY);
        }

        map.put(portable.toString(), portable);

        EntryStealingProcessor processor = new EntryStealingProcessor(portable.toString());
        map.executeOnEntries(processor);

        InternalSerializationService ss = hz.getSerializationService();
        return ss.createPortableReader(processor.stolenEntryData);
    }

    /**
     * Steals a "real" serialised Data of a PortableObject to be as close as possible to real use-case.
     */
    public static class EntryStealingProcessor implements EntryProcessor {

        private final Object key;
        private Data stolenEntryData;

        EntryStealingProcessor(String key) {
            this.key = key;
        }

        @Override
        public EntryProcessor getBackupProcessor() {
            return null;
        }

        @Override
        public Object process(Map.Entry entry) {
            // hack to get rid of de-serialization cost (assuming in-memory-format is BINARY, if it is OBJECT you can replace
            // the null check below with entry.getValue() != null), but works only for versions >= 3.6
            if (key.equals(entry.getKey())) {
                stolenEntryData = ((LazyMapEntry) entry).getValueData();
            }
            return null;
        }
    }

    /**
     * Since the reader has a lot of methods and we want to parametrise the test to invoke each one of them in a generic
     * way, we invoke them using this Invoker that leverages reflection.
     */
    static class Invoker {

        public static <T> T invoke(PortableReader reader, String methodName, String path) {
            return invokeMethod(reader, "read" + methodName, path);
        }

        @SuppressWarnings("unchecked")
        static <T> T invokeMethod(Object object, String methodName, String arg) throws RuntimeException {
            try {
                java.lang.reflect.Method method = object.getClass().getMethod(methodName, String.class);
                return (T) method.invoke(object, arg);
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }
    }
}
