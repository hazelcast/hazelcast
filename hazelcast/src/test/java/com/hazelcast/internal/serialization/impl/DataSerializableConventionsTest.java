/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.BinaryInterface;
import com.hazelcast.spi.AbstractLocalOperation;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.hazelcast.test.ReflectionsHelper.REFLECTIONS;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for classes which are used as Data in the client protocol
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class DataSerializableConventionsTest {

    // verify that any class which is DataSerializable and is not annotated with @BinaryInterface
    // is also an IdentifiedDataSerializable
    // ignored until the conversion to IdentifiedDataSerializable is done
    @Test
    public void test_dataSerializableClasses_areIdentifiedDataSerializable() {
        Set dataSerializableClasses = REFLECTIONS.getSubTypesOf(DataSerializable.class);
        Set allIdDataSerializableClasses = REFLECTIONS.getSubTypesOf(IdentifiedDataSerializable.class);

        dataSerializableClasses.removeAll(allIdDataSerializableClasses);
        // also remove IdentifiedDataSerializable itself
        dataSerializableClasses.remove(IdentifiedDataSerializable.class);

        // locate all classes annotated with BinaryInterface (and their subclasses) and remove those as well
        Set allAnnotatedClasses = REFLECTIONS.getTypesAnnotatedWith(BinaryInterface.class, false);
        dataSerializableClasses.removeAll(allAnnotatedClasses);

        // filter out public classes from public packages (i.e. not "impl" nor "internal" and not annotated with @PrivateApi)
        Set publicClasses = new HashSet();
        for (Object o : dataSerializableClasses) {
            Class klass = (Class) o;
            // if in public packages and not @PrivateApi ==> exclude from checks
            if (!klass.getName().contains("impl") && !klass.getName().contains("internal") && (
                    klass.getAnnotation(PrivateApi.class) == null)) {
                publicClasses.add(klass);
            }
        }
        dataSerializableClasses.removeAll(publicClasses);

        if (dataSerializableClasses.size() > 0) {
            SortedSet<String> nonCompliantClassNames = new TreeSet<String>();
            for (Object o : dataSerializableClasses) {
                nonCompliantClassNames.add(o.toString());
            }
            System.out.println("The following classes are DataSerializable while they should be IdentifiedDataSerializable:");
            // failure - output non-compliant classes to standard output and fail the test
            for (String s : nonCompliantClassNames) {
                System.out.println(s);
            }
            fail("There are " + dataSerializableClasses.size() + " classes which are DataSerializable, not @BinaryInterface-"
                    + "annotated and are not IdentifiedDataSerializable.");
        }
    }

    // fails when IdentifiedDataSerializable classes:
    // - do not have a default no-args constructor
    // - factoryId/id pairs are not unique per class
    @Test
    public void test_identifiedDataSerializables_haveUniqueFactoryAndTypeId()
            throws IllegalAccessException, InvocationTargetException, ClassNotFoundException {
        Set<String> classesWithInstantiationProblems = new TreeSet<String>();
        Set<String> classesThrowingUnsupportedOperationException = new TreeSet<String>();

        Multimap<Integer, Integer> factoryToTypeId = HashMultimap.create();

        Set<Class> identifiedDataSerializables = getIDSProductionConcreteClasses();

        for (Class<? extends IdentifiedDataSerializable> klass : identifiedDataSerializables) {
            // exclude classes which are known to be meant for local use only
            if (!AbstractLocalOperation.class.isAssignableFrom(klass)) {
                // wrap all of this in try-catch, as it is legitimate for some classes to throw UnsupportedOperationException
                try {
                    Constructor<? extends IdentifiedDataSerializable> ctor = klass.getDeclaredConstructor();
                    ctor.setAccessible(true);
                    IdentifiedDataSerializable instance = ctor.newInstance();
                    int factoryId = instance.getFactoryId();
                    int typeId = instance.getId();
                    if (factoryToTypeId.containsEntry(factoryId, typeId)) {
                        fail("Factory-Type ID pair {" + factoryId + ", " + typeId + "} from " + klass.toString() + " is already "
                                + "registered in another type.");
                    } else {
                        factoryToTypeId.put(factoryId, typeId);
                    }
                } catch (UnsupportedOperationException e) {
                    // expected from local operation classes not meant for serialization
                    // gather those and print them to system.out for information at end of test
                    classesThrowingUnsupportedOperationException.add(klass.getName());
                } catch (InstantiationException e) {
                    classesWithInstantiationProblems.add(klass.getName() + " failed with " + e.getMessage());
                } catch (NoSuchMethodException e) {
                    classesWithInstantiationProblems.add(klass.getName() + " failed with " + e.getMessage());
                }
            }
        }

        if (!classesThrowingUnsupportedOperationException.isEmpty()) {
            System.out.println("INFO: " + classesThrowingUnsupportedOperationException.size() + " classes threw "
                    + "UnsupportedOperationException in getFactoryId/getId invocation:");
            for (String className : classesThrowingUnsupportedOperationException) {
                System.out.println(className);
            }
        }

        if (!classesWithInstantiationProblems.isEmpty()) {
            System.out.println("There are " + classesWithInstantiationProblems.size() + " classes which threw an exception while "
                    + "attempting to invoke a default no-args constructor. See console output for exception details. List of "
                    + "problematic classes:");
            for (String className : classesWithInstantiationProblems) {
                System.out.println(className);
            }
            fail("There are " + classesWithInstantiationProblems.size() + " classes which threw an exception while "
                    + "attempting to invoke a default no-args constructor. See test output for exception details.");
        }
    }

    // locates IdentifiedDataSerializable classes via reflection, iterates over them and asserts an instance created by
    // a factory is of the same classes as an instance created via reflection
    @Test
    public void test_identifiedDataSerializables_areInstancesOfSameClass_whenConstructedFromFactory()
            throws IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException,
            ClassNotFoundException {
        Set<Class<? extends DataSerializerHook>> dsHooks = REFLECTIONS.getSubTypesOf(DataSerializerHook.class);
        Map<Integer, DataSerializableFactory> factories = new HashMap<Integer, DataSerializableFactory>();

        for (Class<? extends DataSerializerHook> hookClass : dsHooks) {
            DataSerializerHook dsHook = hookClass.newInstance();
            DataSerializableFactory factory = dsHook.createFactory();
            factories.put(dsHook.getFactoryId(), factory);
        }

        Set<Class> identifiedDataSerializables = getIDSProductionConcreteClasses();
        for (Class<? extends IdentifiedDataSerializable> klass : identifiedDataSerializables) {
            if (AbstractLocalOperation.class.isAssignableFrom(klass)) {
                continue;
            }
            // wrap all of this in try-catch, as it is legitimate for some classes to throw UnsupportedOperationException
            try {
                Constructor<? extends IdentifiedDataSerializable> ctor = klass.getDeclaredConstructor();
                ctor.setAccessible(true);
                IdentifiedDataSerializable instance = ctor.newInstance();
                int factoryId = instance.getFactoryId();
                int typeId = instance.getId();

                if (!factories.containsKey(factoryId)) {
                    fail("Factory with ID " + factoryId + " declared in " + klass + " not found. Is such a factory ID "
                            + "registered?");
                }

                IdentifiedDataSerializable instanceFromFactory = factories.get(factoryId).create(typeId);
                assertNotNull("Factory with ID " + factoryId + " returned null for type with ID " + typeId, instanceFromFactory);
                assertTrue("Factory with ID " + factoryId + " instantiated an object of class " + instanceFromFactory.getClass() +
                        " while expected type was " + instance.getClass(),
                        instanceFromFactory.getClass().equals(instance.getClass()));
            } catch (UnsupportedOperationException e) {
                // expected from local operation classes not meant for serialization
            }
        }
    }

    /**
     * Returns all {@code IdentifiedDataSerializable} concrete classes from production classpath of the currently tested module
     * only, when argument is {@code true}, otherwise from the JVM's classpath.
     * @return
     * @throws ClassNotFoundException
     */
    private Set<Class> getIDSProductionConcreteClasses() throws ClassNotFoundException {
        Set identifiedDataSerializables = REFLECTIONS.getSubTypesOf(IdentifiedDataSerializable.class);
        for (Iterator<Class> iterator = identifiedDataSerializables.iterator();
             iterator.hasNext();) {
            Class<? extends IdentifiedDataSerializable> klass = iterator.next();
            if (klass.isInterface() || Modifier.isAbstract(klass.getModifiers())) {
                iterator.remove();
            }
        }
        return identifiedDataSerializables;
    }
}
