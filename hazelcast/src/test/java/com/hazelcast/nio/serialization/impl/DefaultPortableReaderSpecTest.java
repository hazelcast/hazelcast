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
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.TestPortableFactory;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.Method;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.PrimitivePortable;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.PrimitivePortable.Init.FULL;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.PrimitivePortable.Init.NONE;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTestStructure.PrimitivePortable.Init.NULL;
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
        if (result instanceof Class) {
            expected.expectCause(hasCause(isA((Class) result)));
        }
        assertThat(Invoker.invoke(reader(input), method, path), equalTo(result));
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
        for (Method method : Method.getPrimitives()) {
            if(result instanceof PrimitivePortable) {
                adjustedResult = ((PrimitivePortable)result).getPrimitive(method);
            } else {
                adjustedResult = result;
            }
            Object[] scenario = scenario(input, adjustedResult, method,
                    pathToExplode.replace("primitive_", method.field));
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
    //    scenario(prim(NONE), IllegalArgumentException.class, Method.Byte, "bytes[0]"),
    //    scenario(prim(NULL), IllegalArgumentException.class, Method.Byte, "bytes[1]"),
    //
    static Collection<Object[]> expandPrimitiveArrayScenario(Portable input, PrimitivePortable result, String pathToExplode) {
        List<Object[]> scenarios = new ArrayList<Object[]>();
        // group A:
        for (Method method : Method.getPrimitiveArrays()) {
            String path = pathToExplode.replace("primitiveArray", method.field);
            scenarios.addAll(asList(
                    scenario(input, result.getPrimitiveArray(method), method, path),
                    scenario(input, result.getPrimitiveArray(method), method, path + "[any]")
            ));
        }

        // group B:
        for (Method method : Method.getPrimitives()) {
            String path = pathToExplode.replace("primitiveArray", method.field).replace("_", "s");
            if (result.getPrimitiveArray(method) == null || Array.getLength(result.getPrimitiveArray(method)) == 0) {
                scenarios.add(
                        scenario(input, IllegalArgumentException.class, method, path + "[0]")
                );
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
    //    scenario(prim(NONE), IllegalArgumentException.class, Method.Byte, "bytes[0]"),
    //    scenario(prim(NULL), IllegalArgumentException.class, Method.Byte, "bytes[1]"),
    //
    static Collection<Object[]> expandPortableArrayScenario(Portable input, PrimitivePortable result, String path) {
        List<Object[]> scenarios = new ArrayList<Object[]>();
        // group A:
        Method method = Method.PortableArray;
        scenarios.addAll(asList(
                scenario(input, result, method, path),
                scenario(input, result, method, path.replace("portableArray", "portableArray[any]"))
        ));

//        // group B:
//        for (Method method : Method.getPrimitives()) {
//            String path = pathToExplode.replace("primitiveArray", method.field).replace("_", "s");
//            if (result.getPrimitiveArray(method) == null || Array.getLength(result.getPrimitiveArray(method)) == 0) {
//                scenarios.add(
//                        scenario(input, IllegalArgumentException.class, method, path + "[0]")
//                );
//            } else {
//                scenarios.addAll(asList(
//                        scenario(input, Array.get(result.getPrimitiveArray(method), 0), method, path + "[0]"),
//                        scenario(input, Array.get(result.getPrimitiveArray(method), 1), method, path + "[1]"),
//                        scenario(input, Array.get(result.getPrimitiveArray(method), 2), method, path + "[2]")
//                ));
//            }
//        }
        return scenarios;
    }


    @Parameterized.Parameters(name = "{index}: {0}, read{2}, {3}")
    public static Collection<Object[]> parametrisationData() {

        List<Object[]> result = new ArrayList<Object[]>();

        result.addAll(expandPrimitiveScenario(prim(FULL), prim(FULL), "primitive_"));
        result.addAll(expandPrimitiveArrayScenario(prim(FULL), prim(FULL), "primitiveArray"));
        result.addAll(expandPrimitiveArrayScenario(prim(NONE), prim(NONE), "primitiveArray"));
        result.addAll(expandPrimitiveArrayScenario(prim(NULL), prim(NULL), "primitiveArray"));

        result.addAll(expandPrimitiveScenario(group(prim(FULL)), prim(FULL), "portable.primitive_"));
        result.addAll(expandPrimitiveArrayScenario(group(prim(FULL)), prim(FULL), "portable.primitiveArray"));
        result.addAll(expandPrimitiveArrayScenario(group(prim(NONE)), prim(NONE), "portable.primitiveArray"));
        result.addAll(expandPrimitiveArrayScenario(group(prim(NULL)), prim(NULL), "portable.primitiveArray"));

        GroupPortable fullGroup = group(prim(FULL), prim(FULL), prim(FULL));
        result.addAll(expandPrimitiveScenario(fullGroup, prim(FULL), "portables[0].primitive_"));
        result.addAll(expandPrimitiveScenario(fullGroup, prim(FULL), "portables[1].primitive_"));
        result.addAll(expandPrimitiveScenario(fullGroup, prim(FULL), "portables[2].primitive_"));

        result.addAll(expandPrimitiveArrayScenario(fullGroup, prim(FULL), "portables[0].primitiveArray"));
        result.addAll(expandPrimitiveArrayScenario(fullGroup, prim(FULL), "portables[1].primitiveArray"));
        result.addAll(expandPrimitiveArrayScenario(fullGroup, prim(FULL), "portables[2].primitiveArray"));

        GroupPortable noneGroup = group(prim(NONE), prim(NONE), prim(NONE));
        result.addAll(expandPrimitiveArrayScenario(noneGroup, prim(NONE), "portables[0].primitiveArray"));
        result.addAll(expandPrimitiveArrayScenario(noneGroup, prim(NONE), "portables[1].primitiveArray"));
        result.addAll(expandPrimitiveArrayScenario(noneGroup, prim(NONE), "portables[2].primitiveArray"));

        GroupPortable nullGroup = group(prim(NULL), prim(NULL), prim(NULL));
        result.addAll(expandPrimitiveArrayScenario(nullGroup, prim(NULL), "portables[0].primitiveArray"));
        result.addAll(expandPrimitiveArrayScenario(nullGroup, prim(NULL), "portables[1].primitiveArray"));
        result.addAll(expandPrimitiveArrayScenario(nullGroup, prim(NULL), "portables[2].primitiveArray"));

        GroupPortable emptyArrayGroup = new GroupPortable(new Portable[0]);
        result.addAll(expandPrimitiveScenario(emptyArrayGroup, IllegalArgumentException.class, "portables[0].primitive_"));
        result.addAll(expandPrimitiveScenario(emptyArrayGroup, IllegalArgumentException.class, "portables[1].primitive_"));
        result.addAll(expandPrimitiveScenario(emptyArrayGroup, IllegalArgumentException.class, "portables[2].primitive_"));

        GroupPortable nullArrayGroup = new GroupPortable((Portable[]) null);
        result.addAll(expandPrimitiveScenario(nullArrayGroup, IllegalArgumentException.class, "portables[0].primitive_"));
        result.addAll(expandPrimitiveScenario(nullArrayGroup, IllegalArgumentException.class, "portables[1].primitive_"));
        result.addAll(expandPrimitiveScenario(nullArrayGroup, IllegalArgumentException.class, "portables[2].primitive_"));

        // portable.portable[0-2]

        return result;
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

    static GroupPortable group(PrimitivePortable... portables) {
        return new GroupPortable(portables);
    }

    static PrimitivePortable prim(PrimitivePortable.Init init) {
        return new PrimitivePortable(1, init);
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
