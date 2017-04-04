/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.serialization.PortableHook;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.spi.impl.SpiPortableHook;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.DynamicType;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.hazelcast.test.TestCollectionUtils.setOf;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ServiceLoaderTest extends HazelcastTestSupport {

    @Test
    public void testSkipHooksWithImplementingTheExpectedInterfaceButLoadedByDifferentClassloader() {
        Class<?> otherInterface = newInterface(PortableHook.class.getName());
        ClassLoader otherClassloader = otherInterface.getClassLoader();

        Class<?> otherHook = newClassImplementingInterface("com.hazelcast.internal.serialization.DifferentHook",
                otherInterface, otherClassloader);

        ServiceLoader.ServiceDefinition definition = new ServiceLoader.ServiceDefinition(otherHook.getName(), otherClassloader);
        Set<ServiceLoader.ServiceDefinition> definitions = singleton(definition);
        ServiceLoader.ClassIterator<PortableHook> iterator = new ServiceLoader.ClassIterator<PortableHook>(definitions, PortableHook.class);

        assertFalse(iterator.hasNext());
    }

    @Test(expected = ClassCastException.class)
    public void testFailFastWhenHookDoesNotImplementExpectedInteface() {
        Class<?> otherInterface = newInterface("com.hazelcast.internal.serialization.DifferentInterface");
        ClassLoader otherClassloader = otherInterface.getClassLoader();

        Class<?> otherHook = newClassImplementingInterface("com.hazelcast.internal.serialization.DifferentHook",
                otherInterface, otherClassloader);

        ServiceLoader.ServiceDefinition definition = new ServiceLoader.ServiceDefinition(otherHook.getName(), otherClassloader);
        Set<ServiceLoader.ServiceDefinition> definitions = singleton(definition);
        ServiceLoader.ClassIterator<PortableHook> iterator = new ServiceLoader.ClassIterator<PortableHook>(definitions, PortableHook.class);

        iterator.hasNext();
    }

    @Test
    public void testSkipUnknownClassesStartingFromHazelcastPackage() {
        ServiceLoader.ServiceDefinition definition = new ServiceLoader.ServiceDefinition("com.hazelcast.DoesNotExist", getClass().getClassLoader());
        Set<ServiceLoader.ServiceDefinition> definitions = singleton(definition);
        ServiceLoader.ClassIterator<PortableHook> iterator = new ServiceLoader.ClassIterator<PortableHook>(definitions, PortableHook.class);

        assertFalse(iterator.hasNext());
    }

    @Test(expected = HazelcastException.class)
    public void testFailFastOnUnknownClassesFromNonHazelcastPackage() {
        ServiceLoader.ServiceDefinition definition = new ServiceLoader.ServiceDefinition("non.a.hazelcast.DoesNotExist", getClass().getClassLoader());
        Set<ServiceLoader.ServiceDefinition> definitions = singleton(definition);
        ServiceLoader.ClassIterator<PortableHook> iterator = new ServiceLoader.ClassIterator<PortableHook>(definitions, PortableHook.class);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testSkipHookLoadedByDifferentClassloader() {
        Class<?> otherInterface = newInterface(PortableHook.class.getName());
        ClassLoader otherClassloader = otherInterface.getClassLoader();

        Class<?> otherHook = newClassImplementingInterface("com.hazelcast.internal.serialization.DifferentHook",
                otherInterface, otherClassloader);

        //otherHook is loaded by other classloader -> it should be skipped
        ServiceLoader.ServiceDefinition definition1 = new ServiceLoader.ServiceDefinition(otherHook.getName(), otherClassloader);
        //this hook should be loaded
        ServiceLoader.ServiceDefinition definition2 = new ServiceLoader.ServiceDefinition(SpiPortableHook.class.getName(), SpiPortableHook.class.getClassLoader());

        Set<ServiceLoader.ServiceDefinition> definitions = setOf(definition1, definition2);
        ServiceLoader.ClassIterator<PortableHook> iterator = new ServiceLoader.ClassIterator<PortableHook>(definitions, PortableHook.class);

        assertTrue(iterator.hasNext());
        Class<PortableHook> hook = iterator.next();
        assertEquals(SpiPortableHook.class, hook);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testPrivatePortableHook() {
        String hookName = DummyPrivatePortableHook.class.getName();
        ClassLoader classLoader = DummyPrivatePortableHook.class.getClassLoader();
        ServiceLoader.ServiceDefinition definition = new ServiceLoader.ServiceDefinition(hookName, classLoader);

        ServiceLoader.ClassIterator<PortableHook> classIterator = new ServiceLoader.ClassIterator<PortableHook>(singleton(definition), PortableHook.class);
        ServiceLoader.NewInstanceIterator<PortableHook> instanceIterator = new ServiceLoader.NewInstanceIterator<PortableHook>(classIterator);

        assertTrue(instanceIterator.hasNext());
        DummyPrivatePortableHook hook = (DummyPrivatePortableHook) instanceIterator.next();
        assertNotNull(hook);
    }

    @Test
    public void testPrivateSerializerHook() {
        String hookName = DummyPrivateSerializerHook.class.getName();
        ClassLoader classLoader = DummyPrivateSerializerHook.class.getClassLoader();
        ServiceLoader.ServiceDefinition definition = new ServiceLoader.ServiceDefinition(hookName, classLoader);

        ServiceLoader.ClassIterator<SerializerHook> classIterator = new ServiceLoader.ClassIterator<SerializerHook>(singleton(definition), SerializerHook.class);
        ServiceLoader.NewInstanceIterator<SerializerHook> instanceIterator = new ServiceLoader.NewInstanceIterator<SerializerHook>(classIterator);

        assertTrue(instanceIterator.hasNext());
        DummyPrivateSerializerHook hook = (DummyPrivateSerializerHook) instanceIterator.next();
        assertNotNull(hook);
    }

    private Class<?> newClassImplementingInterface(String classname, Class<?> iface, ClassLoader classLoader) {
        DynamicType.Unloaded<?> otherHookTypeDefinition = new ByteBuddy()
                .subclass(iface)
                .name(classname)
                .make();
        return otherHookTypeDefinition.load(classLoader).getLoaded();
    }

    private Class<?> newInterface(String name) {
        DynamicType.Unloaded<?> otherInterfaceTypeDefinition = new ByteBuddy()
                .makeInterface()
                .name(name)
                .make();
        return otherInterfaceTypeDefinition.load(null).getLoaded();
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ServiceLoader.class);
    }

    @Test
    public void selectingSimpleSingleClassLoader() {
        List<ClassLoader> classLoaders = ServiceLoader.selectClassLoaders(null);
        assertEquals(1, classLoaders.size());
    }

    @Test
    public void selectingSimpleGivenClassLoader() {
        List<ClassLoader> classLoaders = ServiceLoader.selectClassLoaders(new URLClassLoader(new URL[0]));
        assertEquals(2, classLoaders.size());
    }

    @Test
    public void selectingSimpleDifferentThreadContextClassLoader() {
        Thread currentThread = Thread.currentThread();
        ClassLoader tccl = currentThread.getContextClassLoader();
        currentThread.setContextClassLoader(new URLClassLoader(new URL[0]));
        List<ClassLoader> classLoaders = ServiceLoader.selectClassLoaders(null);
        currentThread.setContextClassLoader(tccl);
        assertEquals(2, classLoaders.size());
    }

    @Test
    public void selectingTcclAndGivenClassLoader() {
        Thread currentThread = Thread.currentThread();
        ClassLoader tccl = currentThread.getContextClassLoader();
        currentThread.setContextClassLoader(new URLClassLoader(new URL[0]));
        List<ClassLoader> classLoaders = ServiceLoader.selectClassLoaders(new URLClassLoader(new URL[0]));
        currentThread.setContextClassLoader(tccl);
        assertEquals(3, classLoaders.size());
    }

    @Test
    public void selectingSameTcclAndGivenClassLoader() {
        ClassLoader same = new URLClassLoader(new URL[0]);

        Thread currentThread = Thread.currentThread();
        ClassLoader tccl = currentThread.getContextClassLoader();
        currentThread.setContextClassLoader(same);
        List<ClassLoader> classLoaders = ServiceLoader.selectClassLoaders(same);
        currentThread.setContextClassLoader(tccl);
        assertEquals(2, classLoaders.size());
    }

    @Test
    public void loadServicesSingleClassLoader() throws Exception {
        Class<ServiceLoaderTestInterface> type = ServiceLoaderTestInterface.class;
        String factoryId = "com.hazelcast.ServiceLoaderTestInterface";

        Set<ServiceLoaderTestInterface> implementations = new HashSet<ServiceLoaderTestInterface>();
        Iterator<ServiceLoaderTestInterface> iterator = ServiceLoader.iterator(type, factoryId, null);
        while (iterator.hasNext()) {
            implementations.add(iterator.next());
        }

        assertEquals(1, implementations.size());
    }

    @Test
    public void loadServicesSimpleGivenClassLoader() throws Exception {
        Class<ServiceLoaderTestInterface> type = ServiceLoaderTestInterface.class;
        String factoryId = "com.hazelcast.ServiceLoaderTestInterface";

        ClassLoader given = new URLClassLoader(new URL[0]);

        Set<ServiceLoaderTestInterface> implementations = new HashSet<ServiceLoaderTestInterface>();
        Iterator<ServiceLoaderTestInterface> iterator = ServiceLoader.iterator(type, factoryId, given);
        while (iterator.hasNext()) {
            implementations.add(iterator.next());
        }

        assertEquals(1, implementations.size());
    }

    @Test
    public void loadServicesSimpleDifferentThreadContextClassLoader() throws Exception {
        Class<ServiceLoaderTestInterface> type = ServiceLoaderTestInterface.class;
        String factoryId = "com.hazelcast.ServiceLoaderTestInterface";

        Thread current = Thread.currentThread();
        ClassLoader tccl = current.getContextClassLoader();
        current.setContextClassLoader(new URLClassLoader(new URL[0]));

        Set<ServiceLoaderTestInterface> implementations = new HashSet<ServiceLoaderTestInterface>();
        Iterator<ServiceLoaderTestInterface> iterator = ServiceLoader.iterator(type, factoryId, null);
        while (iterator.hasNext()) {
            implementations.add(iterator.next());
        }

        current.setContextClassLoader(tccl);
        assertEquals(1, implementations.size());
    }

    @Test
    public void loadServicesTcclAndGivenClassLoader() throws Exception {
        Class<ServiceLoaderTestInterface> type = ServiceLoaderTestInterface.class;
        String factoryId = "com.hazelcast.ServiceLoaderTestInterface";

        ClassLoader given = new URLClassLoader(new URL[0]);

        Thread current = Thread.currentThread();
        ClassLoader tccl = current.getContextClassLoader();
        current.setContextClassLoader(new URLClassLoader(new URL[0]));

        Set<ServiceLoaderTestInterface> implementations = new HashSet<ServiceLoaderTestInterface>();
        Iterator<ServiceLoaderTestInterface> iterator = ServiceLoader.iterator(type, factoryId, given);
        while (iterator.hasNext()) {
            implementations.add(iterator.next());
        }

        current.setContextClassLoader(tccl);
        assertEquals(1, implementations.size());
    }

    @Test
    public void loadServicesSameTcclAndGivenClassLoader() throws Exception {
        Class<ServiceLoaderTestInterface> type = ServiceLoaderTestInterface.class;
        String factoryId = "com.hazelcast.ServiceLoaderTestInterface";

        ClassLoader same = new URLClassLoader(new URL[0]);

        Thread current = Thread.currentThread();
        ClassLoader tccl = current.getContextClassLoader();
        current.setContextClassLoader(same);

        Set<ServiceLoaderTestInterface> implementations = new HashSet<ServiceLoaderTestInterface>();
        Iterator<ServiceLoaderTestInterface> iterator = ServiceLoader.iterator(type, factoryId, same);
        while (iterator.hasNext()) {
            implementations.add(iterator.next());
        }

        current.setContextClassLoader(tccl);
        assertEquals(1, implementations.size());
    }

    @Test
    public void loadServicesWithSpaceInURL() throws Exception {
        Class<ServiceLoaderSpacesTestInterface> type = ServiceLoaderSpacesTestInterface.class;
        String factoryId = "com.hazelcast.ServiceLoaderSpacesTestInterface";

        URL url = new URL(ClassLoader.getSystemResource("test with spaces").toExternalForm().replace("%20", " ") + "/");
        ClassLoader given = new URLClassLoader(new URL[]{url});

        Set<ServiceLoaderSpacesTestInterface> implementations = new HashSet<ServiceLoaderSpacesTestInterface>();
        Iterator<ServiceLoaderSpacesTestInterface> iterator = ServiceLoader.iterator(type, factoryId, given);
        while (iterator.hasNext()) {
            implementations.add(iterator.next());
        }

        assertEquals(1, implementations.size());
    }

    public interface ServiceLoaderTestInterface {
    }

    public static class ServiceLoaderTestInterfaceImpl implements ServiceLoaderTestInterface {
    }

    public interface ServiceLoaderSpacesTestInterface {
    }

    public static class ServiceLoaderSpacesTestInterfaceImpl implements ServiceLoaderSpacesTestInterface {
    }

    private static class DummyPrivatePortableHook implements PortableHook {
        @Override
        public int getFactoryId() {
            return 0;
        }

        @Override
        public PortableFactory createFactory() {
            return null;
        }

        @Override
        public Collection<ClassDefinition> getBuiltinDefinitions() {
            return null;
        }
    }

    private static class DummyPrivateSerializerHook implements SerializerHook {

        @Override
        public Class getSerializationType() {
            return null;
        }

        @Override
        public Serializer createSerializer() {
            return null;
        }

        @Override
        public boolean isOverwritable() {
            return false;
        }
    }
}
