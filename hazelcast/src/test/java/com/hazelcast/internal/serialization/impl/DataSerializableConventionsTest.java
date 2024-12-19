/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cp.internal.CPMemberInfo;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.internal.locksupport.operations.LocalLockCleanupOperation;
import com.hazelcast.internal.partition.operation.FinalizeMigrationOperation;
import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.jet.impl.MasterJobContext.SnapshotRestoreEdge;
import com.hazelcast.map.impl.operation.MapPartitionDestroyOperation;
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
import com.hazelcast.spi.impl.operationservice.AbstractLocalOperation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.vector.impl.DataVectorDocument;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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
import static com.hazelcast.test.ReflectionsHelper.concreteSubTypesOf;
import static com.hazelcast.test.ReflectionsHelper.subTypesOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
@Category(QuickTest.class)
public class DataSerializableConventionsTest {
    private static final String JET_PACKAGE = "com.hazelcast.jet";

    /**
     * Subclasses of classes in the white list are not taken into account for
     * conventions tests because <ol>
     * <li> they inherit Serializable from a parent class and cannot implement
     *      IdentifiedDataSerializable due to the unavailability of default constructor,
     * <li> they purposefully break conventions to fix a known issue, or
     * <li> their factory/class ID depend on the underlying data, such as QueryDataType.
     */
    private final Set<Class<?>> classWhiteList;
    private final Set<Class<?>> hookWhiteList;
    private final Set<String> packageWhiteList;
    /**
     * Classes contained in these packages have serialization implemented in EE modules.
     */
    private final Set<Package> enterprisePackages;

    public DataSerializableConventionsTest() {
        classWhiteList = Collections.unmodifiableSet(getWhitelistedClasses());
        packageWhiteList = Collections.unmodifiableSet(getWhitelistedPackageNames());
        hookWhiteList = Collections.unmodifiableSet(getWhitelistedHookClasses());
        enterprisePackages = Collections.unmodifiableSet(getEnterprisePackages());
    }

    /**
     * Verifies that any class which is {@link DataSerializable} and is not annotated with {@link BinaryInterface}
     * or {@link SerializableByConvention} is also an {@link IdentifiedDataSerializable}.
     */
    @Test
    public void test_dataSerializableClasses_areIdentifiedDataSerializable() {
        Set<Class<? extends DataSerializable>> dataSerializableClasses = REFLECTIONS.get(
                concreteSubTypesOf(DataSerializable.class)
                        .filter(c -> !IdentifiedDataSerializable.class.isAssignableFrom(c))
                        .filter(c -> !c.isAnnotationPresent(BinaryInterface.class))
                        .filter(c -> !c.isAnnotationPresent(SerializableByConvention.class)));

        if (!dataSerializableClasses.isEmpty()) {
            SortedSet<String> nonCompliantClassNames = new TreeSet<>();
            for (Class<?> c : dataSerializableClasses) {
                if (!inheritsFromWhiteListedClass(c)) {
                    nonCompliantClassNames.add(c.toString());
                }
            }
            assertThat(nonCompliantClassNames).isEmpty();
        }
    }

    /**
     * Verifies that any class which is {@link Serializable} and is not annotated with {@link BinaryInterface}
     * or {@link SerializableByConvention} is also an {@link IdentifiedDataSerializable}.
     */
    @Test
    public void test_serializableClasses_areIdentifiedDataSerializable() {
        Set<Class<? extends Serializable>> serializableClasses = REFLECTIONS.get(
                concreteSubTypesOf(Serializable.class)
                        .filter(c -> !IdentifiedDataSerializable.class.isAssignableFrom(c))
                        .filter(c -> !c.isAnnotationPresent(BinaryInterface.class))
                        .filter(c -> !c.isAnnotationPresent(SerializableByConvention.class)));

        if (!serializableClasses.isEmpty()) {
            SortedSet<String> nonCompliantClassNames = new TreeSet<>();
            for (Class<?> c : serializableClasses) {
                if (!inheritsFromWhiteListedClass(c)) {
                    nonCompliantClassNames.add(c.toString());
                }
            }
            assertThat(nonCompliantClassNames).isEmpty();
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

        Set<Class<? extends IdentifiedDataSerializable>> identifiedDataSerializables = REFLECTIONS.get(
                concreteSubTypesOf(IdentifiedDataSerializable.class)
                        .filter(c -> !classWhiteList.contains(c)));

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
                } catch (InvocationTargetException e) {
                    // expected from local operation classes not meant for serialization
                    // gather those and print them to system.out for information at end of test
                    if (!(e.getCause() instanceof UnsupportedOperationException)) {
                        throw e;
                    }
                    classesThrowingUnsupportedOperationException.add(klass.getName());
                } catch (InstantiationException | NoSuchMethodException e) {
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

        assertThat(classesWithInstantiationProblems).isEmpty();
    }

    /**
     * Locates {@link IdentifiedDataSerializable} classes via reflection, iterates over them and asserts an instance created by
     * a factory is of the same classes as an instance created via reflection.
     */
    @Test
    public void test_identifiedDataSerializables_areInstancesOfSameClass_whenConstructedFromFactory() throws Exception {
        Set<Class<? extends DataSerializerHook>> hookClasses = REFLECTIONS.get(
                subTypesOf(DataSerializerHook.class)
                    .filter(c -> !hookWhiteList.contains(c)));

        Set<DataSerializerHook> hooks = new HashSet<>();
        Map<Integer, DataSerializableFactory> factories = new HashMap<>();

        for (Class<? extends DataSerializerHook> hookClass : hookClasses) {
            DataSerializerHook dsHook = hookClass.getDeclaredConstructor().newInstance();
            DataSerializableFactory factory = dsHook.createFactory();
            factories.put(dsHook.getFactoryId(), factory);
            hooks.add(dsHook);
        }
        for (DataSerializerHook hook : hooks) {
            hook.afterFactoriesCreated(factories);
        }

        Set<Class<? extends IdentifiedDataSerializable>> identifiedDataSerializables = REFLECTIONS.get(
                concreteSubTypesOf(IdentifiedDataSerializable.class)
                        .filter(c -> !classWhiteList.contains(c)));

        for (Class<? extends IdentifiedDataSerializable> klass : identifiedDataSerializables) {
            if (AbstractLocalOperation.class.isAssignableFrom(klass)) {
                continue;
            }
            if (isReadOnlyConfig(klass)) {
                continue;
            }
            if (enterprisePackages.contains(klass.getPackage())) {
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
                assertEquals("Factory with ID " + factoryId + " instantiated an object of " + instanceFromFactory.getClass()
                        + " while expected type was " + instance.getClass(), instanceFromFactory.getClass(), instance.getClass());
            } catch (UnsupportedOperationException e) {
                // expected from local operation classes not meant for serialization
            } catch (InvocationTargetException e) {
                // expected from local operation classes not meant for serialization
                if (!(e.getCause() instanceof UnsupportedOperationException)) {
                    throw e;
                }
            }
        }
    }

    private boolean isReadOnlyConfig(Class<? extends IdentifiedDataSerializable> klass) {
        String className = klass.getName();
        return className.endsWith("ReadOnly")
                && (className.contains("Config") || className.contains("WanReplicationRef"));
    }

    private boolean inheritsFromWhiteListedClass(Class<?> klass) {
        String className = klass.getName();

        for (String packageName : packageWhiteList) {
            if (className.startsWith(packageName)) {
                return true;
            }
        }

        for (Class<?> superclass : classWhiteList) {
            if (superclass.isAssignableFrom(klass)) {
                return true;
            }
        }
        return false;
    }

    protected Set<String> getWhitelistedPackageNames() {
        return Collections.singleton(JET_PACKAGE);
    }

    protected Set<? extends Package> getEnterprisePackages() {
        return Set.of(CPMemberInfo.class.getPackage(),
                    LogEntry.class.getPackage(),
                    DataVectorDocument.class.getPackage());
    }

    /**
     * Returns the set of classes excluded from the conventions tests.
     */
    protected Set<Class<?>> getWhitelistedClasses() {
        Set<Class<?>> whiteList = new HashSet<>();
        whiteList.add(BoundedRangePredicate.class);
        whiteList.add(CachedQueryEntry.class);
        whiteList.add(CompositeEqualPredicate.class);
        whiteList.add(CompositeRangePredicate.class);
        whiteList.add(EvaluatePredicate.class);
        whiteList.add(EventObject.class);
        whiteList.add(FinalizeMigrationOperation.class);
        whiteList.add(LocalLockCleanupOperation.class);
        whiteList.add(MapPartitionDestroyOperation.class);
        whiteList.add(Permission.class);
        whiteList.add(PermissionCollection.class);
        whiteList.add(SkipIndexPredicate.class);
        whiteList.add(SnapshotRestoreEdge.class);
        whiteList.add(Throwable.class);
        whiteList.add(WanMapEntryView.class);
        try {
            // These can't be accessed directly since they are private.
            whiteList.add(Class.forName("com.hazelcast.query.impl.predicates.CompositeIndexVisitor$Output"));
            whiteList.add(Class.forName("com.hazelcast.query.impl.predicates.RangeVisitor$Ranges"));
            whiteList.add(Class.forName("com.hazelcast.internal.partition.operation.BeforePromotionOperation"));
            whiteList.add(Class.forName("com.hazelcast.internal.partition.operation.FinalizePromotionOperation"));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return whiteList;
    }

    protected Set<Class<?>> getWhitelistedHookClasses() {
        return new HashSet<>();
    }
}
