package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import static org.junit.Assert.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DynamicConfigurationAwareConfigTest {

    @Test
    public void testDecorateAllPublicMethodsFromTest() {
        // this test makes sure when you add a new method into Config class
        // then you also adds it into the Dynamic Configuration Aware decorator.

        // in other words: if this test is failing then update the DynamicConfigurationAwareConfig
        Method[] methods = DynamicConfigurationAwareConfig.class.getMethods();
        for (Method method : methods) {
            if (isMethodStatic(method)) {
                continue;
            }
            if (isMethodDeclaredByClass(method, Object.class)) {
                //let's skip methods like wait() or notify() - declared directly in the Object class
                continue;
            }

            //all other public method should be overriden by the dynamic config aware decorator
            if (!isMethodDeclaredByClass(method, DynamicConfigurationAwareConfig.class)) {
                Class<?> declaringClass = method.getDeclaringClass();
                fail("Method " + method + " is declared by " + declaringClass + " whilst it should be" +
                        " declared by " + DynamicConfigurationAwareConfig.class);
            }
        }
    }

    private static boolean isMethodStatic(Method method) {
        return Modifier.isStatic(method.getModifiers());
    }

    private static boolean isMethodDeclaredByClass(Method method, Class<?> expectedDeclaringClass) {
        Class<?> actualDeclaringClass = method.getDeclaringClass();
        return expectedDeclaringClass.equals(actualDeclaringClass);
    }

}