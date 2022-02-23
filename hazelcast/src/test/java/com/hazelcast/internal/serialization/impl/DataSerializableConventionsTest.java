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

package com.hazelcast.internal.serialization.impl;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.hazelcast.internal.locksupport.operations.LocalLockCleanupOperation;
import com.hazelcast.internal.partition.operation.FinalizeMigrationOperation;
import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.jet.impl.MasterJobContext;
import com.hazelcast.sql.impl.expression.SymbolExpression;
import com.hazelcast.map.impl.wan.WanMapEntryView;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.CachedQueryEntry;
import com.hazelcast.query.impl.predicates.BoundedRangePredicate;
import com.hazelcast.query.impl.predicates.CompositeEqualPredicate;
import com.hazelcast.query.impl.predicates.CompositeRangePredicate;
import com.hazelcast.query.impl.predicates.EvaluatePredicate;
import com.hazelcast.query.impl.predicates.SkipIndexPredicate;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.impl.operationservice.AbstractLocalOperation;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.security.Permission;
import java.security.PermissionCollection;
import java.util.Collections;
import java.util.EventObject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.hazelcast.test.ReflectionsHelper.REFLECTIONS;
import static com.hazelcast.test.ReflectionsHelper.filterNonConcreteClasses;
import static com.hazelcast.test.ReflectionsHelper.filterNonHazelcastClasses;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests to verify serializable classes conventions are observed.
 * Each conventions test scans the classpath (excluding test classes)
 * and tests <b>concrete</b> classes which implement (directly or
 * transitively) {@code Serializable} or {@code DataSerializable}
 * interface, then verifies that it's either annotated with {@link
 * BinaryInterface}, is excluded from conventions tests by being annotated
 * with {@link SerializableByConvention} or they also implement {@code
 * IdentifiedDataSerializable}. Additionally, tests whether IDS instances
 * obtained from DS factories have the same ID as the one reported by
 * their `getClassId` method and that F_ID/ID combinations are unique.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class DataSerializableConventionsTest {

    private static final String JET_PACKAGE = "com.hazelcast.jet";

    // subclasses of classes in the white list are not taken into account for
    // conventions tests. Reasons:
    // - they inherit Serializable from a parent class and cannot implement
    // IdentifiedDataSerializable due to unavailability of default constructor.
    // - they purposefully break conventions to fix a known issue
    private final Set<Class> classWhiteList;
    private final Set<String> packageWhiteList;

    public DataSerializableConventionsTest() {
        classWhiteList = Collections.unmodifiableSet(getWhitelistedClasses());
        packageWhiteList = Collections.unmodifiableSet(getWhitelistedPackageNames());
    }

    /**
     * Verifies that any class which is {@link DataSerializable} and is not annotated with {@link BinaryInterface}
     * is also an {@link IdentifiedDataSerializable}.
     */
    @Test
    public void test_dataSerializableClasses_areIdentifiedDataSerializable() {
        Set<Class<? extends DataSerializable>> dataSerializableClasses = REFLECTIONS.getSubTypesOf(DataSerializable.class);
        Set<Class<? extends IdentifiedDataSerializable>> allIdDataSerializableClasses
                = REFLECTIONS.getSubTypesOf(IdentifiedDataSerializable.class);

        dataSerializableClasses.removeAll(allIdDataSerializableClasses);
        // also remove IdentifiedDataSerializable itself
        dataSerializableClasses.remove(IdentifiedDataSerializable.class);
        // do not check abstract classes & interfaces
        filterNonConcreteClasses(dataSerializableClasses);

        // locate all classes annotated with BinaryInterface and remove those as well
        Set<?> allAnnotatedClasses = REFLECTIONS.getTypesAnnotatedWith(BinaryInterface.class, true);
        dataSerializableClasses.removeAll(allAnnotatedClasses);

        // exclude @SerializableByConvention classes
        Set<?> serializableByConventions = REFLECTIONS.getTypesAnnotatedWith(SerializableByConvention.class, true);
        dataSerializableClasses.removeAll(serializableByConventions);

        if (dataSerializableClasses.size() > 0) {
            SortedSet<String> nonCompliantClassNames = new TreeSet<>();
            for (Object o : dataSerializableClasses) {
                if (!inheritsFromWhiteListedClass((Class) o)) {
                    nonCompliantClassNames.add(o.toString());
                }
            }
            if (!nonCompliantClassNames.isEmpty()) {
                System.out.println("The following classes are DataSerializable while they should be IdentifiedDataSerializable:");
                // failure - output non-compliant classes to standard output and fail the test
                for (String s : nonCompliantClassNames) {
                    System.out.println(s);
                }
                fail("There are " + dataSerializableClasses.size() + " classes which are DataSerializable, not @BinaryInterface-"
                        + "annotated and are not IdentifiedDataSerializable.");
            }
        }
    }

    /**
     * Verifies that any class which is {@link Serializable} and is not annotated with {@link BinaryInterface}
     * is also an {@link IdentifiedDataSerializable}.
     */
    @Test
    public void test_serializableClasses_areIdentifiedDataSerializable() {
        Set<Class<? extends Serializable>> serializableClasses = REFLECTIONS.getSubTypesOf(Serializable.class);
        Set<Class<? extends IdentifiedDataSerializable>> allIdDataSerializableClasses
                = REFLECTIONS.getSubTypesOf(IdentifiedDataSerializable.class);

        serializableClasses.removeAll(allIdDataSerializableClasses);

        // do not check non hazelcast classes & interfaces
        filterNonHazelcastClasses(serializableClasses);
        // do not check abstract classes & interfaces
        filterNonConcreteClasses(serializableClasses);

        // locate all classes annotated with BinaryInterface and remove those as well
        Set<?> allAnnotatedClasses = REFLECTIONS.getTypesAnnotatedWith(BinaryInterface.class, true);
        serializableClasses.removeAll(allAnnotatedClasses);

        // exclude @SerializableByConvention classes
        Set<?> serializableByConventions = REFLECTIONS.getTypesAnnotatedWith(SerializableByConvention.class, true);
        serializableClasses.removeAll(serializableByConventions);

        if (serializableClasses.size() > 0) {
            SortedSet<String> nonCompliantClassNames = new TreeSet<String>();
            for (Object o : serializableClasses) {
                if (!inheritsFromWhiteListedClass((Class) o)) {
                    nonCompliantClassNames.add(o.toString());
                }
            }
            if (!nonCompliantClassNames.isEmpty()) {
                System.out.println("The following classes are Serializable and should be IdentifiedDataSerializable:");
                // failure - output non-compliant classes to standard output and fail the test
                for (String s : nonCompliantClassNames) {
                    System.out.println(s);
                }
                fail("There are " + nonCompliantClassNames.size() + " classes which are Serializable, not @BinaryInterface-"
                        + "annotated and are not IdentifiedDataSerializable.");
            }
        }
    }

    /**
     * Fails when {@link IdentifiedDataSerializable} classes:
     * - do not have a default no-args constructor
     * - factoryId/id pairs are not unique per class
     */
    @Test
    public void test_identifiedDataSerializables_haveUniqueFactoryAndTypeId() throws Exception {
        Set<String> classesWithInstantiationProblems = new TreeSet<>();
        Set<String> classesThrowingUnsupportedOperationException = new TreeSet<>();

        Multimap<Integer, Integer> factoryToTypeId = HashMultimap.create();

        Set<Class<? extends IdentifiedDataSerializable>> identifiedDataSerializables = getIDSConcreteClasses();
        for (Class<? extends IdentifiedDataSerializable> klass : identifiedDataSerializables) {
            // exclude classes which are known to be meant for local use only
            if (!AbstractLocalOperation.class.isAssignableFrom(klass) && !isReadOnlyConfig(klass)) {
                // wrap all of this in try-catch, as it is legitimate for some classes to throw UnsupportedOperationException
                try {
                    Constructor<? extends IdentifiedDataSerializable> ctor = klass.getDeclaredConstructor();
                    ctor.setAccessible(true);
                    IdentifiedDataSerializable instance = ctor.newInstance();
                    int factoryId = instance.getFactoryId();
                    int typeId = instance.getClassId();
                    if (factoryToTypeId.containsEntry(factoryId, typeId)) {
                        fail("Factory-Type ID pair {" + factoryId + ", " + typeId + "} from " + klass + " is already"
                                + " registered in another type.");
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
            System.out.println("INFO: " + classesThrowingUnsupportedOperationException.size() + " classes threw"
                    + " UnsupportedOperationException in getFactoryId/getClassId invocation:");
            for (String className : classesThrowingUnsupportedOperationException) {
                System.out.println(className);
            }
        }

        if (!classesWithInstantiationProblems.isEmpty()) {
            System.out.println("There are " + classesWithInstantiationProblems.size() + " classes which threw an exception while"
                    + " attempting to invoke a default no-args constructor. See console output for exception details."
                    + " List of problematic classes:");
            for (String className : classesWithInstantiationProblems) {
                System.out.println(className);
            }
            fail("There are " + classesWithInstantiationProblems.size() + " classes which threw an exception while"
                    + " attempting to invoke a default no-args constructor. See test output for exception details.");
        }
    }

    /**
     * Locates {@link IdentifiedDataSerializable} classes via reflection, iterates over them and asserts an instance created by
     * a factory is of the same classes as an instance created via reflection.
     */
    @Test
    public void test_identifiedDataSerializables_areInstancesOfSameClass_whenConstructedFromFactory() throws Exception {
        Set<Class<? extends DataSerializerHook>> dsHooks = REFLECTIONS.getSubTypesOf(DataSerializerHook.class);
        Map<Integer, DataSerializableFactory> factories = new HashMap<Integer, DataSerializableFactory>();

        for (Class<? extends DataSerializerHook> hookClass : dsHooks) {
            DataSerializerHook dsHook = hookClass.newInstance();
            DataSerializableFactory factory = dsHook.createFactory();
            factories.put(dsHook.getFactoryId(), factory);
        }

        Set<Class<? extends IdentifiedDataSerializable>> identifiedDataSerializables = getIDSConcreteClasses();
        for (Class<? extends IdentifiedDataSerializable> klass : identifiedDataSerializables) {
            if (AbstractLocalOperation.class.isAssignableFrom(klass)) {
                continue;
            }
            if (isReadOnlyConfig(klass)) {
                continue;
            }
            // wrap all of this in try-catch, as it is legitimate for some classes to throw UnsupportedOperationException
            try {
                Constructor<? extends IdentifiedDataSerializable> ctor = klass.getDeclaredConstructor();
                ctor.setAccessible(true);
                IdentifiedDataSerializable instance = ctor.newInstance();
                int factoryId = instance.getFactoryId();
                int typeId = instance.getClassId();

                if (!factories.containsKey(factoryId)) {
                    fail("Factory with ID " + factoryId + " declared in " + klass + " not found."
                            + " Is such a factory ID registered?");
                }

                IdentifiedDataSerializable instanceFromFactory = factories.get(factoryId).create(typeId);
                assertNotNull("Factory with ID " + factoryId + " returned null for type with ID " + typeId, instanceFromFactory);
                assertTrue("Factory with ID " + factoryId + " instantiated an object of " + instanceFromFactory.getClass()
                                + " while expected type was " + instance.getClass(),
                        instanceFromFactory.getClass().equals(instance.getClass()));
            } catch (UnsupportedOperationException ignored) {
                // expected from local operation classes not meant for serialization
            }
        }
    }

    private boolean isReadOnlyConfig(Class<? extends IdentifiedDataSerializable> klass) {
        String className = klass.getName();
        return className.endsWith("ReadOnly")
                && (className.contains("Config") || className.contains("WanReplicationRef"));
    }

    /**
     * Returns all concrete classes which implement {@link IdentifiedDataSerializable} located by
     * {@link com.hazelcast.test.ReflectionsHelper#REFLECTIONS}.
     *
     * @return a set of all {@link IdentifiedDataSerializable} classes
     */
    private Set<Class<? extends IdentifiedDataSerializable>> getIDSConcreteClasses() {
        Set<Class<? extends IdentifiedDataSerializable>> identifiedDataSerializables
                = REFLECTIONS.getSubTypesOf(IdentifiedDataSerializable.class);
        filterNonConcreteClasses(identifiedDataSerializables);
        identifiedDataSerializables.removeAll(classWhiteList);
        return identifiedDataSerializables;
    }

    /**
     * @return {@code true} when klass has a superclass that implements or is itself of type {@code inheritedClass}
     */
    private boolean inheritsClassFromPublicClass(Class klass, Class inheritedClass) {
        // check interfaces implemented by klass: if one of these implements inheritedClass and is public, then true
        Class[] interfaces = klass.getInterfaces();
        if (interfaces != null) {
            for (Class implementedInterface : interfaces) {
                if (implementedInterface.equals(inheritedClass)) {
                    return false;
                } else if (inheritedClass.isAssignableFrom(implementedInterface) && isPublicClass(implementedInterface)) {
                    return true;
                }
            }
        }

        // use hierarchyIteratingClass to iterate up the klass hierarchy
        Class hierarchyIteratingClass = klass;
        while (hierarchyIteratingClass.getSuperclass() != null) {
            if (hierarchyIteratingClass.getSuperclass().equals(inheritedClass)) {
                return true;
            }
            if (inheritedClass.isAssignableFrom(hierarchyIteratingClass.getSuperclass())
                    && isPublicClass(hierarchyIteratingClass.getSuperclass())) {
                return true;
            }
            hierarchyIteratingClass = hierarchyIteratingClass.getSuperclass();
        }
        return false;
    }

    private boolean isPublicClass(Class klass) {
        return !klass.getName().contains(".impl.") && !klass.getName().contains(".internal.")
                && klass.getAnnotation(PrivateApi.class) == null;
    }

    private boolean inheritsFromWhiteListedClass(Class klass) {
        String className = klass.getName();

        for (String packageName : packageWhiteList) {
            if (className.startsWith(packageName)) {
                return true;
            }
        }

        for (Class superclass : classWhiteList) {
            if (superclass.isAssignableFrom(klass)) {
                return true;
            }
        }
        return false;
    }

    protected Set<String> getWhitelistedPackageNames() {
        return Collections.singleton(JET_PACKAGE);
    }

    /**
     * Returns the set of classes excluded from the conventions tests.
     */
    protected Set<Class> getWhitelistedClasses() {
        Set<Class> whiteList = new HashSet<Class>();
        whiteList.add(EventObject.class);
        whiteList.add(Throwable.class);
        whiteList.add(Permission.class);
        whiteList.add(PermissionCollection.class);
        whiteList.add(WanMapEntryView.class);
        whiteList.add(SkipIndexPredicate.class);
        whiteList.add(BoundedRangePredicate.class);
        whiteList.add(CompositeRangePredicate.class);
        whiteList.add(CompositeEqualPredicate.class);
        whiteList.add(EvaluatePredicate.class);
        whiteList.add(Converter.class);
        whiteList.add(CachedQueryEntry.class);
        whiteList.add(LocalLockCleanupOperation.class);
        whiteList.add(FinalizeMigrationOperation.class);
        whiteList.add(SymbolExpression.class);
        whiteList.add(MasterJobContext.SnapshotRestoreEdge.class);
        try {
            // these can't be accessed through the meta class since they are private
            whiteList.add(Class.forName("com.hazelcast.query.impl.predicates.CompositeIndexVisitor$Output"));
            whiteList.add(Class.forName("com.hazelcast.query.impl.predicates.RangeVisitor$Ranges"));
            whiteList.add(Class.forName("com.hazelcast.internal.partition.operation.BeforePromotionOperation"));
            whiteList.add(Class.forName("com.hazelcast.internal.partition.operation.FinalizePromotionOperation"));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return whiteList;
    }
}
