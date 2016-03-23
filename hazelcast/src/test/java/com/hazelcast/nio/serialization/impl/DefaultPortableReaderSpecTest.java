package com.hazelcast.nio.serialization.impl;


import com.hazelcast.config.Config;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.internal.serialization.impl.SerializationServiceV1;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.map.impl.LazyMapEntry;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ExceptionUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.GroupPortable;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.Method;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.NestedGroupPortable;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.PrimitivePortable;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.PrimitivePortable.Init.FULL;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.PrimitivePortable.Init.NONE;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.PrimitivePortable.Init.NULL;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.TestPortableFactory;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertThat;
import static org.junit.internal.matchers.ThrowableCauseMatcher.hasCause;

@RunWith(Parameterized.class)
@Category(QuickTest.class)
public class DefaultPortableReaderSpecTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private static final PrimitivePortable P_NON_EMPTY = new PrimitivePortable(0, PrimitivePortable.Init.FULL);
    private static final GroupPortable G_NON_EMPTY = group(FULL);
    private static final NestedGroupPortable N_NON_EMPTY = nested(new Portable[]{G_NON_EMPTY, G_NON_EMPTY});

    private Portable input;
    private Object result;
    private Method method;
    private String path;

    public DefaultPortableReaderSpecTest(Portable input, Object result, Method method, String path) {
        this.input = input;
        this.result = result;
        this.method = method;
        this.path = path;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void primitive_array() throws IOException {


        Object resultToMatch = result;
        if (result instanceof Class) {
            // expected exception case
            expected.expectCause(hasCause(isA((Class) result)));
        } else if (result instanceof List) {
            // just convenience -> if result is a list if will be compared to an array, so it has to be converted
            resultToMatch = ((List) resultToMatch).toArray();
        }

        String desc = "Running test case:\n";
        desc += "path:\t" + path + "\n";
        desc += "method:\tread" + method.name() + "\n";
        desc += "result:\t" + resultToMatch + "\n";
        desc += "input:\t" + input + "\n";
        System.out.println(desc);

        assertThat(Invoker.invoke(reader(input), method, path), equalTo(resultToMatch));
    }

    static class Invoker {
        public static <T> T invoke(PortableReader reader, Method method, String path) {
            return invokeMethod(reader, "read" + method.name(), path);
        }

        public static <T> T invokeMethod(Object object, String methodName, String arg) throws RuntimeException {
            try {
                java.lang.reflect.Method method = object.getClass().getMethod(methodName, String.class);
                return (T) method.invoke(object, arg);
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }
    }

    // Expands test cases for primitive non-array data types.
    // Word "primitive_" is replaced by each primitive type and the scenario is expanded to:
    //
    //    scenario(input, result.byte_, Method.Byte, adjustedPath + "byte_"),
    //    scenario(input, result.short_, Method.Short, adjustedPath + "short_"),
    //    scenario(input, result.int_, Method.Int, adjustedPath + "int_"),
    //    scenario(input, result.long_, Method.Long, adjustedPath + "long_"),
    //    scenario(input, result.float_, Method.Float, adjustedPath + "float_"),
    //    scenario(input, result.double_, Method.Double, adjustedPath + "double_"),
    //    scenario(input, result.boolean_, Method.Boolean, adjustedPath + "boolean_"),
    //    scenario(input, result.char_, Method.Char, adjustedPath + "char_"),
    //    scenario(input, result.string_, Method.UTF, adjustedPath + "string_"),
    //
    static Collection<Object[]> expandPrimitiveScenario(Portable input, Object result, String pathToExplode) {
        List<Object[]> scenarios = new ArrayList<Object[]>();
        Object adjustedResult;
        String tokenToReplace = "primitive_";
        if (pathToExplode.contains("primitiveUTF_")) {
            tokenToReplace = "primitiveUTF_";
        }
        for (Method method : Method.getPrimitives(tokenToReplace.contains("UTF"))) {
            if (result instanceof PrimitivePortable) {
                adjustedResult = ((PrimitivePortable) result).getPrimitive(method);
            } else if (result == null && method != Method.UTF) {
                adjustedResult = IllegalArgumentException.class;
            } else {
                adjustedResult = result;
            }
            Object[] scenario = scenario(input, adjustedResult, method,
                    pathToExplode.replace(tokenToReplace, method.field));
            scenarios.add(scenario);
        }
        return scenarios;
    }

    // Expands test cases for primitive array data types.
    // Word "primitiveArray" is replaced by each primitive array type and the scenario is expanded to for each type:
    //
    // group A:
    //    scenario(prim(FULL), prim(FULL).bytes, Method.ByteArray, "bytes"),
    //    scenario(prim(NONE), prim(NONE).bytes, Method.ByteArray, "bytes"),
    //    scenario(prim(NULL), prim(NULL).bytes, Method.ByteArray, "bytes"),
    //
    //    scenario(prim(FULL), prim(FULL).bytes, Method.ByteArray, "bytes[any]"),
    //    scenario(prim(NONE), prim(NONE).bytes, Method.ByteArray, "bytes[any]"),
    //    scenario(prim(NULL), prim(NULL).bytes, Method.ByteArray, "bytes[any]"),
    //
    // group B:
    //    scenario(prim(FULL), prim(FULL).bytes[0], Method.Byte, "bytes[0]"),
    //    scenario(prim(FULL), prim(FULL).bytes[1], Method.Byte, "bytes[1]"),
    //    scenario(prim(FULL), prim(FULL).bytes[2], Method.Byte, "bytes[2]"),
    //
    //    for all primitives apart from UTF (exception expected)
    //    scenario(prim(NONE), IllegalArgumentException.class, Method.Byte, "bytes[0]"),
    //    scenario(prim(NULL), IllegalArgumentException.class, Method.Byte, "bytes[1]"),
    //
    //    for UTF (null expected)
    //    scenario(prim(NONE), null, Method.UTF, "strings[0]"),
    //    scenario(prim(NULL), null, Method.UTF, "strings[1]"),
    //
    static Collection<Object[]> expandPrimitiveArrayScenario(Portable input, PrimitivePortable result, String pathToExplode) {
        List<Object[]> scenarios = new ArrayList<Object[]>();
        // group A:
        for (Method method : Method.getPrimitiveArrays()) {
            String path = pathToExplode.replace("primitiveArray", method.field);
            scenarios.addAll(asList(
                    scenario(input, result != null ? result.getPrimitiveArray(method) : result, method, path),
                    scenario(input, result != null ? result.getPrimitiveArray(method) : result, method, path + "[any]")
            ));
        }

        // group B:
        for (Method method : Method.getPrimitives(true)) {
            String path = pathToExplode.replace("primitiveArray", method.field).replace("_", "s");
            if (result == null || result.getPrimitiveArray(method) == null || Array.getLength(result.getPrimitiveArray(method)) == 0) {
                if (method.equals(Method.UTF)) {
                    scenarios.add(
                            scenario(input, null, method, path + "[0]")
                    );
                } else {
                    scenarios.add(
                            scenario(input, IllegalArgumentException.class, method, path + "[0]")
                    );
                }
            } else {
                scenarios.addAll(asList(
                        scenario(input, Array.get(result.getPrimitiveArray(method), 0), method, path + "[0]"),
                        scenario(input, Array.get(result.getPrimitiveArray(method), 1), method, path + "[1]"),
                        scenario(input, Array.get(result.getPrimitiveArray(method), 2), method, path + "[2]")
                ));
            }
        }
        return scenarios;
    }

    static Collection<Object[]> expandPortableArrayPrimitiveScenario(Portable input, GroupPortable result, String pathToExplode) {
        List<Object[]> scenarios = new ArrayList<Object[]>();
        for (String token : asList("0", "1", "2", "any")) {

            String tokenToReplace = "primitive_";
            if (pathToExplode.contains("primitiveUTF_")) {
                tokenToReplace = "primitiveUTF_";
            }
            String path = pathToExplode.replace("portableArray", "portables[" + token + "]");
            if (token.equals("any")) {
                for (Method method : Method.getPrimitives(tokenToReplace.contains("UTF"))) {
                    List resultToMatch = new ArrayList();
                    int portableCount = 0;
                    try {
                        portableCount = result.portables.length;
                    } catch (NullPointerException ex) {
                    }
                    for (int i = 0; i < portableCount; i++) {
                        PrimitivePortable portable = (PrimitivePortable) result.portables[i];
                        resultToMatch.add(portable.getPrimitive(method));
                    }
                    if (result == null || result.portables == null) {
                        resultToMatch = null;
                    }

                    Object[] scenario = scenario(input, resultToMatch, Method.getArrayMethodFor(method),
                            path.replace(tokenToReplace, method.field));
                    scenarios.add(scenario);
                }
            } else {

                for (Method method : Method.getPrimitives(tokenToReplace.contains("UTF"))) {
                    Object resultToMatch = null;
                    try {
                        PrimitivePortable portable = (PrimitivePortable) result.portables[Integer.parseInt(token)];
                        resultToMatch = portable.getPrimitive(method);
                    } catch (NullPointerException ex) {
                    } catch (IndexOutOfBoundsException ex) {
                    }

                    if (method != Method.UTF) {
                        if (result == null || result.portables == null || result.portables.length == 0) {
                            resultToMatch = IllegalArgumentException.class;
                        }
                    }

                    Object[] scenario = scenario(input, resultToMatch, method,
                            path.replace(tokenToReplace, method.field));
                    scenarios.add(scenario);
                }
            }
        }
        return scenarios;
    }

    @Parameterized.Parameters(name = "{index}: {0}, read{2}, {3}")
    public static Collection<Object[]> parametrisationData() {
        List<Object[]> result = new ArrayList<Object[]>();

        directPrimitiveScenarios(result);
        fromPortableToPrimitiveScenarios(result);
        fromPortableArrayToPrimitiveScenarios(result);
        fromPortableToPortableToPrimitiveScenarios(result);
        fromPortableToPortableArrayToPrimitiveScenarios(result);
        fromPortableArrayToPortableArrayToPrimitiveScenarios(result);
        edgeCaseScenarios(result);

        return result;
    }

    private static void directPrimitiveScenarios(List<Object[]> result) {
        // ----------------------------------------------------------------------------------------------------------
        // DIRECT primitive and primitive-array access
        // ----------------------------------------------------------------------------------------------------------

        // FULLy initialised primitive objects accessed directly
        result.addAll(expandPrimitiveScenario(prim(FULL), prim(FULL), "primitiveUTF_"));

        // primitive arrays accessed directly (arrays are fully initialised, empty and null)
        result.addAll(expandPrimitiveArrayScenario(prim(FULL), prim(FULL), "primitiveArray"));
        result.addAll(expandPrimitiveArrayScenario(prim(NONE), prim(NONE), "primitiveArray"));
        result.addAll(expandPrimitiveArrayScenario(prim(NULL), prim(NULL), "primitiveArray"));
    }

    private static void fromPortableToPrimitiveScenarios(List<Object[]> result) {
        // ----------------------------------------------------------------------------------------------------------
        // from PORTABLE primitive and primitive-array access
        // ----------------------------------------------------------------------------------------------------------
        // FULLy initialised primitive objects accessed from portable
        result.addAll(expandPrimitiveScenario(group(prim(FULL)), prim(FULL), "portable.primitiveUTF_"));

        // primitive arrays accessed from portable (arrays are fully initialised, empty and null)
        result.addAll(expandPrimitiveArrayScenario(group(prim(FULL)), prim(FULL), "portable.primitiveArray"));
        result.addAll(expandPrimitiveArrayScenario(group(prim(NONE)), prim(NONE), "portable.primitiveArray"));
        result.addAll(expandPrimitiveArrayScenario(group(prim(NULL)), prim(NULL), "portable.primitiveArray"));
    }

    private static void fromPortableArrayToPrimitiveScenarios(List<Object[]> result) {
        // ----------------------------------------------------------------------------------------------------------
        // from PORTABLE-ARRAY primitive and primitive-array access
        // ----------------------------------------------------------------------------------------------------------

        // FULLy initialised primitive objects accessed from portable stored in array
        GroupPortable fullGroupVarious = group(prim(1, FULL), prim(10, FULL), prim(100, FULL));
        result.addAll(expandPortableArrayPrimitiveScenario(fullGroupVarious, fullGroupVarious, "portableArray.primitiveUTF_"));

        GroupPortable fullEmptyNullGroup = group(prim(1, FULL), prim(10, NONE), prim(100, NULL));
        result.addAll(expandPortableArrayPrimitiveScenario(fullEmptyNullGroup, fullEmptyNullGroup, "portableArray.primitiveUTF_"));

        // empty or null portable array de-referenced further
        GroupPortable nullArrayGroup = new GroupPortable((Portable[]) null);
        result.addAll(expandPortableArrayPrimitiveScenario(nullArrayGroup, nullArrayGroup, "portableArray.primitiveUTF_"));

        GroupPortable emptyArrayGroup = new GroupPortable(new Portable[0]);
        result.addAll(expandPortableArrayPrimitiveScenario(emptyArrayGroup, emptyArrayGroup, "portableArray.primitiveUTF_"));


        // FULLy initialised primitive arrays accessed from portable stored in array
        GroupPortable fullGroup = group(prim(FULL), prim(FULL), prim(FULL));
        result.addAll(expandPrimitiveArrayScenario(fullGroup, prim(FULL), "portables[0].primitiveArray"));
        result.addAll(expandPrimitiveArrayScenario(fullGroup, prim(FULL), "portables[1].primitiveArray"));
        result.addAll(expandPrimitiveArrayScenario(fullGroup, prim(FULL), "portables[2].primitiveArray"));

        // EMPTY primitive arrays accessed from portable stored in array
        GroupPortable noneGroup = group(prim(NONE), prim(NONE), prim(NONE));
        result.addAll(expandPrimitiveArrayScenario(noneGroup, prim(NONE), "portables[0].primitiveArray"));
        result.addAll(expandPrimitiveArrayScenario(noneGroup, prim(NONE), "portables[1].primitiveArray"));
        result.addAll(expandPrimitiveArrayScenario(noneGroup, prim(NONE), "portables[2].primitiveArray"));

        // NULL primitive arrays accessed from portable stored in array
        GroupPortable nullGroup = group(prim(NULL), prim(NULL), prim(NULL));
        result.addAll(expandPrimitiveArrayScenario(nullGroup, prim(NULL), "portables[0].primitiveArray"));
        result.addAll(expandPrimitiveArrayScenario(nullGroup, prim(NULL), "portables[1].primitiveArray"));
        result.addAll(expandPrimitiveArrayScenario(nullGroup, prim(NULL), "portables[2].primitiveArray"));

        // EMPTY portable array -> de-referenced further for primitive access
        result.addAll(expandPrimitiveScenario(emptyArrayGroup, IllegalArgumentException.class, "portables[0].primitive_"));
        result.addAll(expandPrimitiveScenario(emptyArrayGroup, IllegalArgumentException.class, "portables[1].primitive_"));
        result.addAll(expandPrimitiveScenario(emptyArrayGroup, IllegalArgumentException.class, "portables[2].primitive_"));
        result.add(scenario(emptyArrayGroup, null, Method.UTF, "portables[0].string_"));
        result.add(scenario(emptyArrayGroup, null, Method.UTF, "portables[1].string_"));
        result.add(scenario(emptyArrayGroup, null, Method.UTF, "portables[2].string_"));

        // EMPTY portable array -> de-referenced further for array access
        result.addAll(expandPrimitiveArrayScenario(emptyArrayGroup, null, "portables[0].primitiveArray"));
        result.addAll(expandPrimitiveArrayScenario(emptyArrayGroup, null, "portables[1].primitiveArray"));
        result.addAll(expandPrimitiveArrayScenario(emptyArrayGroup, null, "portables[2].primitiveArray"));

        // NULL portable array -> de-referenced further for primitive access
        result.addAll(expandPrimitiveScenario(nullArrayGroup, IllegalArgumentException.class, "portables[0].primitive_"));
        result.addAll(expandPrimitiveScenario(nullArrayGroup, IllegalArgumentException.class, "portables[1].primitive_"));
        result.addAll(expandPrimitiveScenario(nullArrayGroup, IllegalArgumentException.class, "portables[2].primitive_"));
        result.add(scenario(nullArrayGroup, null, Method.UTF, "portables[0].string_"));
        result.add(scenario(nullArrayGroup, null, Method.UTF, "portables[1].string_"));
        result.add(scenario(nullArrayGroup, null, Method.UTF, "portables[2].string_"));

        // EMPTY portable array -> de-referenced further for array access
        result.addAll(expandPrimitiveArrayScenario(nullArrayGroup, null, "portables[0].primitiveArray"));
        result.addAll(expandPrimitiveArrayScenario(nullArrayGroup, null, "portables[1].primitiveArray"));
        result.addAll(expandPrimitiveArrayScenario(nullArrayGroup, null, "portables[2].primitiveArray"));
    }

    private static void fromPortableToPortableToPrimitiveScenarios(List<Object[]> result) {
        // ----------------------------------------------------------------------------------------------------------
        // from PORTABLE to PORTABLE access + further
        // ----------------------------------------------------------------------------------------------------------

        // FULLy initialised primitive objects accessed from portable stored in array
        NestedGroupPortable nestedFullGroup = nested(group(prim(1, FULL), prim(10, FULL), prim(100, FULL)));
        result.addAll(asList(
                scenario(nestedFullGroup, (nestedFullGroup.portable), Method.Portable, "portable"),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portable)).portable, Method.Portable, "portable.portable")
        ));
        result.addAll(expandPrimitiveScenario(nestedFullGroup, ((GroupPortable) nestedFullGroup.portable).portable, "portable.portable.primitiveUTF_"));

        NestedGroupPortable nestedFullEmptyNullGroup = nested(group(prim(1, FULL), prim(10, NONE), prim(100, NULL)));
        result.addAll(expandPrimitiveScenario(nestedFullEmptyNullGroup, ((GroupPortable) nestedFullEmptyNullGroup.portable).portable, "portable.portable.primitiveUTF_"));

        // empty or null portable array de-referenced further
        NestedGroupPortable nestedNullArrayGroup = nested(new GroupPortable((Portable[]) null));
        result.addAll(asList(
                scenario(nestedNullArrayGroup, (nestedNullArrayGroup.portable), Method.Portable, "portable"),
                scenario(nestedNullArrayGroup, null, Method.Portable, "portable.portable")
        ));
        result.addAll(expandPrimitiveScenario(nestedNullArrayGroup, null, "portable.portable.primitiveUTF_"));
        // TODO primitive array case
        // TODO primitive array case with [any]

        NestedGroupPortable nestedNull = nested(new Portable[0]);
        result.addAll(asList(
                scenario(nestedNull, null, Method.Portable, "portable"),
                scenario(nestedNull, null, Method.Portable, "portable.portable")
        ));
        result.addAll(expandPrimitiveScenario(nestedNull, null, "portable.portable.primitiveUTF_"));
        // TODO primitive array case
        // TODO primitive array case with [any]

    }

    private static void fromPortableToPortableArrayToPrimitiveScenarios(List<Object[]> result) {
        // ----------------------------------------------------------------------------------------------------------
        // from PORTABLE to PORTABLE_ARRAY access + further
        // ----------------------------------------------------------------------------------------------------------

        // FULLy initialised primitive objects accessed from portable stored in array
        NestedGroupPortable nestedFullGroup = nested(group(prim(1, FULL), prim(10, FULL), prim(100, FULL)));
        result.addAll(asList(
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portable)).portables, Method.PortableArray, "portable.portables"),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portable)).portables, Method.PortableArray, "portable.portables[any]"),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portable)).portables[0], Method.Portable, "portable.portables[0]"),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portable)).portables[1], Method.Portable, "portable.portables[1]"),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portable)).portables[2], Method.Portable, "portable.portables[2]"),
                scenario(nestedFullGroup, null, Method.Portable, "portable.portables[12]")
        ));
        result.addAll(
                expandPortableArrayPrimitiveScenario(nestedFullGroup, (GroupPortable) nestedFullGroup.portable, "portable.portableArray.primitiveUTF_")
        );

        NestedGroupPortable nestedfullEmptyNullGroup = nested(group(prim(1, FULL), prim(10, NONE), prim(100, NULL)));
        result.addAll(
                expandPortableArrayPrimitiveScenario(nestedfullEmptyNullGroup, (GroupPortable) nestedfullEmptyNullGroup.portable, "portable.portableArray.primitiveUTF_")
        );

        // empty or null portable array de-referenced further
        NestedGroupPortable nestedNullArrayGroup = nested(new GroupPortable((Portable[]) null));
        result.addAll(asList(
                scenario(nestedNullArrayGroup, null, Method.PortableArray, "portable.portables"),
                scenario(nestedNullArrayGroup, null, Method.PortableArray, "portable.portables[any]"),
                scenario(nestedNullArrayGroup, null, Method.Portable, "portable.portables[0]"),
                scenario(nestedNullArrayGroup, null, Method.Portable, "portable.portables[1]"),
                scenario(nestedNullArrayGroup, null, Method.Portable, "portable.portables[2]")
        ));
        result.addAll(
                expandPortableArrayPrimitiveScenario(nestedNullArrayGroup, (GroupPortable) nestedNullArrayGroup.portable, "portable.portableArray.primitiveUTF_")
        );

        NestedGroupPortable nestedEmptyArrayGroup = nested(new GroupPortable(new Portable[0]));
        result.addAll(asList(
                scenario(nestedEmptyArrayGroup, new Portable[0], Method.PortableArray, "portable.portables"),
                scenario(nestedEmptyArrayGroup, new Portable[0], Method.PortableArray, "portable.portables[any]"),
                scenario(nestedEmptyArrayGroup, null, Method.Portable, "portable.portables[0]"),
                scenario(nestedEmptyArrayGroup, null, Method.Portable, "portable.portables[1]"),
                scenario(nestedEmptyArrayGroup, null, Method.Portable, "portable.portables[2]")
        ));
        result.addAll(
                expandPortableArrayPrimitiveScenario(nestedEmptyArrayGroup, (GroupPortable) nestedEmptyArrayGroup.portable, "portable.portableArray.primitiveUTF_")
        );

        NestedGroupPortable nestedEmpty = nested(new GroupPortable[0]);
        result.addAll(asList(
                scenario(nestedEmpty, null, Method.PortableArray, "portable.portables"),
                scenario(nestedEmpty, null, Method.PortableArray, "portable.portables[any]"),
                scenario(nestedEmpty, null, Method.Portable, "portable.portables[0]"),
                scenario(nestedEmpty, null, Method.Portable, "portable.portables[1]"),
                scenario(nestedEmpty, null, Method.Portable, "portable.portables[2]")
        ));
        result.addAll(
                expandPortableArrayPrimitiveScenario(nestedEmpty, (GroupPortable) nestedEmpty.portable, "portable.portableArray.primitiveUTF_")
        );

        NestedGroupPortable nestedNull = nested((GroupPortable[]) null);
        result.addAll(asList(
                scenario(nestedNull, null, Method.PortableArray, "portable.portables"),
                scenario(nestedNull, null, Method.PortableArray, "portable.portables[any]"),
                scenario(nestedNull, null, Method.Portable, "portable.portables[0]"),
                scenario(nestedNull, null, Method.Portable, "portable.portables[1]"),
                scenario(nestedNull, null, Method.Portable, "portable.portables[2]")
        ));
        result.addAll(
                expandPortableArrayPrimitiveScenario(nestedNull, (GroupPortable) nestedNull.portable, "portable.portableArray.primitiveUTF_")
        );
    }

    private static void fromPortableArrayToPortableArrayToPrimitiveScenarios(List<Object[]> result) {
        // ----------------------------------------------------------------------------------------------------------
        // from PORTABLE to PORTABLE_ARRAY access + further
        // ----------------------------------------------------------------------------------------------------------

        // FULLy initialised primitive objects accessed from portable stored in array
        NestedGroupPortable nestedFullGroup = nested(group(prim(1, FULL), prim(10, FULL), prim(100, FULL)));
        result.addAll(asList(
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portables[0])).portables, Method.PortableArray, "portables[0].portables"),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portables[0])).portables, Method.PortableArray, "portables[0].portables[any]"),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portables[0])).portables[0], Method.Portable, "portables[0].portables[0]"),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portables[0])).portables[1], Method.Portable, "portables[0].portables[1]"),
                scenario(nestedFullGroup, ((GroupPortable) (nestedFullGroup.portables[0])).portables[2], Method.Portable, "portables[0].portables[2]"),
                scenario(nestedFullGroup, null, Method.Portable, "portables[0].portables[12]")
        ));
        result.addAll(
                expandPortableArrayPrimitiveScenario(nestedFullGroup, (GroupPortable) nestedFullGroup.portable, "portables[0].portableArray.primitiveUTF_")
        );

        NestedGroupPortable nestedFullEmptyNullGroup = nested(group(prim(1, FULL), prim(10, NONE), prim(100, NULL)));
        result.addAll(
                expandPortableArrayPrimitiveScenario(nestedFullEmptyNullGroup, (GroupPortable) nestedFullEmptyNullGroup.portables[0], "portables[0].portableArray.primitiveUTF_")
        );

        // empty or null portable array de-referenced further
        NestedGroupPortable nestedNullArrayGroup = nested(new GroupPortable((Portable[]) null));
        result.addAll(asList(
                scenario(nestedNullArrayGroup, null, Method.PortableArray, "portables[0].portables"),
                scenario(nestedNullArrayGroup, null, Method.PortableArray, "portables[0].portables[any]"),
                scenario(nestedNullArrayGroup, null, Method.PortableArray, "portables[any].portables[any]"),
                scenario(nestedNullArrayGroup, null, Method.PortableArray, "portables[any].portables[0]"),
                scenario(nestedNullArrayGroup, null, Method.Portable, "portables[0].portables[0]"),
                scenario(nestedNullArrayGroup, null, Method.Portable, "portables[0].portables[1]"),
                scenario(nestedNullArrayGroup, null, Method.Portable, "portables[0].portables[2]")
        ));
        result.addAll(
                expandPortableArrayPrimitiveScenario(nestedNullArrayGroup, (GroupPortable) nestedNullArrayGroup.portable, "portables[0].portableArray.primitiveUTF_")
        );

        NestedGroupPortable nestedEmptyArrayGroup = nested(new GroupPortable(new Portable[0]));
        result.addAll(asList(
                scenario(nestedEmptyArrayGroup, new Portable[0], Method.PortableArray, "portables[0].portables"),
                scenario(nestedEmptyArrayGroup, new Portable[0], Method.PortableArray, "portables[0].portables[any]"),
                scenario(nestedEmptyArrayGroup, new Portable[0], Method.PortableArray, "portables[any].portables[any]"),
                scenario(nestedEmptyArrayGroup, null, Method.PortableArray, "portables[any].portables[0]"),
                scenario(nestedEmptyArrayGroup, null, Method.Portable, "portables[0].portables[0]"),
                scenario(nestedEmptyArrayGroup, null, Method.Portable, "portables[0].portables[1]"),
                scenario(nestedEmptyArrayGroup, null, Method.Portable, "portables[0].portables[2]")
        ));
        result.addAll(
                expandPortableArrayPrimitiveScenario(nestedEmptyArrayGroup, (GroupPortable) nestedEmptyArrayGroup.portable, "portables[0].portableArray.primitiveUTF_")
        );

        //
        // Currently:
        // - if [any] is done on non-null array, the result will never be null, but an empty or non-empty array
        // - if [any] is done on a null array, the result will be null
        //
        // TODO: check if that's acceptable
        //
        NestedGroupPortable nestedEmpty = nested(new GroupPortable[0]);
        result.addAll(asList(
                scenario(nestedEmpty, null, Method.PortableArray, "portables[0].portables"),
                scenario(nestedEmpty, null, Method.PortableArray, "portables[0].portables[any]"),
                scenario(nestedEmpty, new Portable[0], Method.PortableArray, "portables[any].portables[any]"),
                scenario(nestedEmpty, new Portable[0], Method.PortableArray, "portables[any].portables[0]"), // ???
                scenario(nestedEmpty, null, Method.Portable, "portables[0].portables[0]"),
                scenario(nestedEmpty, null, Method.Portable, "portables[0].portables[1]"),
                scenario(nestedEmpty, null, Method.Portable, "portables[0].portables[2]")
        ));
        result.addAll(
                expandPortableArrayPrimitiveScenario(nestedEmpty, (GroupPortable) nestedEmpty.portable, "portables[0].portableArray.primitiveUTF_")
        );

        NestedGroupPortable nestedNull = nested((GroupPortable[]) null);
        result.addAll(asList(
                scenario(nestedNull, null, Method.PortableArray, "portables[0].portables"),
                scenario(nestedNull, null, Method.PortableArray, "portables[0].portables[any]"),
                scenario(nestedNull, null, Method.PortableArray, "portables[any].portables[any]"),
                scenario(nestedNull, null, Method.PortableArray, "portables[any].portables[0]"),
                scenario(nestedNull, null, Method.Portable, "portables[0].portables[0]"),
                scenario(nestedNull, null, Method.Portable, "portables[0].portables[1]"),
                scenario(nestedNull, null, Method.Portable, "portables[0].portables[2]")
        ));
        result.addAll(
                expandPortableArrayPrimitiveScenario(nestedNull, (GroupPortable) nestedNull.portable, "portables[0].portableArray.primitiveUTF_")
        );
    }


    private static void edgeCaseScenarios(List<Object[]> result) {
        // nested [any] queries with primitives
        // [any] for portable types -> including nulls
        // [any] for primitive types -> if nulls -> exception handling
    }

    //
    // Test data structure utilities
    //
    static GroupPortable group(PrimitivePortable.Init init) {
        PrimitivePortable[] portables;
        if (init == FULL) {
            int count = 3;
            portables = new PrimitivePortable[count];
            for (int i = 0; i < count; i++) {
                portables[i] = new PrimitivePortable(i, FULL);
            }
        } else if (init == NULL) {
            portables = null;
        } else {
            portables = new PrimitivePortable[0];
        }
        return new GroupPortable(portables);
    }

    static GroupPortable group(PrimitivePortable portable) {
        return new GroupPortable(portable);
    }

    static NestedGroupPortable nested(GroupPortable portable) {
        return new NestedGroupPortable(portable);
    }

    static NestedGroupPortable nested(Portable[] portables) {
        return new NestedGroupPortable(portables);
    }

    static GroupPortable group(PrimitivePortable... portables) {
        return new GroupPortable(portables);
    }

    static PrimitivePortable prim(PrimitivePortable.Init init) {
        return new PrimitivePortable(1, init);
    }

    static PrimitivePortable prim(int seed, PrimitivePortable.Init init) {
        return new PrimitivePortable(seed, init);
    }

    public static Object[] scenario(Portable input, Object result, Method method, String path) {
        return new Object[]{input, result, method, path};
    }

    public static Collection<Object[]> test(Object[]... scenarios) {
        List<Object[]> result = new ArrayList<Object[]>();
        for (Object[] scenario : scenarios) {
            result.add(scenario);
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
        IMap map = hz.getMap("stealingMap");

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

        SerializationServiceV1 ss = (SerializationServiceV1) hz.getSerializationService();
        return ss.createPortableReader(processor.stolenEntryData);
    }

    //
    // Main goal is to steal a "real" serialised Data of a PortableObject to be as close as possible to real use-case
    //
    public static class EntryStealingProcessor extends AbstractEntryProcessor {
        private final Object key;
        private Data stolenEntryData;

        public EntryStealingProcessor(String key) {
            super(false);
            this.key = key;
        }

        public Object process(Map.Entry entry) {
            // Hack to get rid of de-serialization cost.
            // And assuming in-memory-format is BINARY, if it is OBJECT you can replace
            // the null check below with entry.getValue() != null
            // Works only for versions >= 3.6
            if (key.equals(entry.getKey())) {
                stolenEntryData = (Data) ((LazyMapEntry) entry).getValueData();
            }
            return null;
        }
    }

}
