package com.hazelcast.internal.adapter;

import com.hazelcast.internal.adapter.DataStructureAdapter.DataStructureMethods;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.junit.Assert.fail;

/**
 * Checks that {@link DataStructureMethods} contains unique method signatures which are defined in {@link DataStructureAdapter}.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DataStructureMethodsTest {

    @Test
    public void testThatAllDataStructureMethodsAreFoundAndReferencedJustOnce() {
        String adapterClassName = DataStructureAdapter.class.getSimpleName();
        String methodsClassName = DataStructureMethods.class.getSimpleName();

        Map<Method, DataStructureMethods> knownMethods = new HashMap<Method, DataStructureMethods>();
        for (DataStructureMethods method : DataStructureMethods.values()) {
            try {
                Method adapterMethod = DataStructureAdapter.class.getMethod(method.getMethodName(), method.getParameterTypes());
                if (knownMethods.containsKey(adapterMethod)) {
                    fail(format("%s::%s(%s) is referenced by %s.%s, but was already referenced by %s.%s",
                            adapterClassName, method.getMethodName(), method.getParameterTypeString(), methodsClassName, method,
                            methodsClassName, knownMethods.get(adapterMethod)));
                }
                knownMethods.put(adapterMethod, method);
            } catch (NoSuchMethodException e) {
                fail(format("%s::%s(%s) is referenced by %s.%s, but could not be found!",
                        adapterClassName, method.getMethodName(), method.getParameterTypeString(), methodsClassName, method));
            }
        }
    }
}
