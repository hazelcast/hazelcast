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

package com.hazelcast.internal.util;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.portable.PortableHook;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.DynamicType;
import org.apache.catalina.Context;
import org.apache.catalina.WebResourceRoot;
import org.apache.catalina.Wrapper;
import org.apache.catalina.startup.Tomcat;
import org.apache.catalina.webresources.DirResourceSet;
import org.apache.catalina.webresources.StandardRoot;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.nio.IOUtil.toByteArray;
import static com.hazelcast.test.TestCollectionUtils.setOf;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ServiceLoaderTest extends HazelcastTestSupport {

    @Test
    public void selectClassLoaders_whenTCCL_isNull_thenDoNotSelectNullClassloader() {
        Thread.currentThread().setContextClassLoader(null);
        ClassLoader dummyClassLoader = new URLClassLoader(new URL[0]);
        List<ClassLoader> classLoaders = ServiceLoader.selectClassLoaders(dummyClassLoader);

        assertNotContains(classLoaders, null);
    }

    @Test
    public void selectClassLoaders_whenPassedClassLoaderIsisNull_thenDoNotSelectNullClassloader() {
        Thread.currentThread().setContextClassLoader(null);
        List<ClassLoader> classLoaders = ServiceLoader.selectClassLoaders(null);

        assertNotContains(classLoaders, null);
    }

    @Test
    public void testMultipleClassloaderLoadsTheSameClass() throws Exception {
        ClassLoader parent = this.getClass().getClassLoader();

        //child classloader will steal bytecode from the parent and will define classes on its own
        ClassLoader childLoader = new StealingClassloader(parent);

        Class<?> interfaceClass = childLoader.loadClass(DataSerializerHook.class.getName());
        Iterator<? extends Class<?>> iterator
                = ServiceLoader.classIterator(interfaceClass, "com.hazelcast.DataSerializerHook", childLoader);

        //make sure some hook were found.
        assertTrue(iterator.hasNext());

        while (iterator.hasNext()) {
            Class<?> hook = iterator.next();
            assertEquals(childLoader, hook.getClassLoader());
        }
    }

    @Test
    public void testMultipleClassloaderLoadsTheSameClass_fromParentClassLoader() throws Exception {
        // Given: a parent & child class loaders that can load same classes
        //        and same factoryId from same URL
        // When:  requesting classes loaded by parent class loader from the child loader
        // Then:  classes from parent class loader are returned

        // Use case:
        //  - Hazelcast jars in both tomcat/lib and tomcat/webapps/foo/lib
        //  - Tomcat configured with hazelcast session manager (also in tomcat/lib)
        //  - Session manager is being initialized for foo context: service loading
        //    uses foo webapp classloader to locate all resources by factoryId.
        //    It locates factoryId's both in foo/lib and tomcat/lib but returned
        //    classes must be loaded from the parent classloader (from the
        //    Hazelcast jar in tomcat/lib, same as the HazelcastInstance class that
        //    is being started by the session manager).
        ClassLoader parent = this.getClass().getClassLoader();
        ClassLoader childLoader = new StealingClassloader(parent);
        // ensure parent and child loader load separate Class objects for same class name
        assertNotSame(parent.loadClass(DataSerializerHook.class.getName()),
                        childLoader.loadClass(DataSerializerHook.class.getName()));

        // request from childLoader the classes that implement DataSerializerHook, as loaded by parent
        Iterator<? extends Class<?>> iterator
                = ServiceLoader.classIterator(DataSerializerHook.class, "com.hazelcast.DataSerializerHook", childLoader);

        //make sure hooks were found.
        assertTrue(iterator.hasNext());

        // ensure all hooks are loaded from parent classloader
        while (iterator.hasNext()) {
            Class<?> hook = iterator.next();
            assertSame(parent, hook.getClassLoader());
        }
    }

    @Test
    public void testHookDeduplication() {
        Class<?> hook = newClassImplementingInterface("com.hazelcast.internal.serialization.SomeHook",
                PortableHook.class, PortableHook.class.getClassLoader());

        ClassLoader parentClassloader = hook.getClassLoader();

        //child classloader delegating everything to its parent
        URLClassLoader childClassloader = new URLClassLoader(new URL[]{}, parentClassloader);

        ServiceLoader.ServiceDefinition definition1 = new ServiceLoader.ServiceDefinition(hook.getName(), parentClassloader);
        //the definition loaded by the child classloader -> it only delegates to the parent -> it's a duplicated
        ServiceLoader.ServiceDefinition definition2 = new ServiceLoader.ServiceDefinition(hook.getName(), childClassloader);

        Set<ServiceLoader.ServiceDefinition> definitions = setOf(definition1, definition2);
        ServiceLoader.ClassIterator<PortableHook> iterator
                = new ServiceLoader.ClassIterator<PortableHook>(definitions, PortableHook.class);

        assertTrue(iterator.hasNext());
        Class<PortableHook> hookFromIterator = iterator.next();
        assertEquals(hook, hookFromIterator);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testSkipHooksWithImplementingTheExpectedInterfaceButLoadedByDifferentClassloader() {
        Class<?> otherInterface = newInterface(PortableHook.class.getName());
        ClassLoader otherClassloader = otherInterface.getClassLoader();

        Class<?> otherHook = newClassImplementingInterface("com.hazelcast.internal.serialization.DifferentHook",
                otherInterface, otherClassloader);

        ServiceLoader.ServiceDefinition definition = new ServiceLoader.ServiceDefinition(otherHook.getName(), otherClassloader);
        Set<ServiceLoader.ServiceDefinition> definitions = singleton(definition);
        ServiceLoader.ClassIterator<PortableHook> iterator
                = new ServiceLoader.ClassIterator<PortableHook>(definitions, PortableHook.class);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testFailFastWhenHookDoesNotImplementExpectedInteface() {
        Class<?> otherInterface = newInterface("com.hazelcast.internal.serialization.DifferentInterface");
        ClassLoader otherClassloader = otherInterface.getClassLoader();

        Class<?> otherHook = newClassImplementingInterface("com.hazelcast.internal.serialization.DifferentHook",
                otherInterface, otherClassloader);

        ServiceLoader.ServiceDefinition definition = new ServiceLoader.ServiceDefinition(otherHook.getName(), otherClassloader);
        Set<ServiceLoader.ServiceDefinition> definitions = singleton(definition);
        ServiceLoader.ClassIterator<PortableHook> iterator
                = new ServiceLoader.ClassIterator<PortableHook>(definitions, PortableHook.class);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testSkipUnknownClassesStartingFromHazelcastPackage() {
        ServiceLoader.ServiceDefinition definition
                = new ServiceLoader.ServiceDefinition("com.hazelcast.DoesNotExist", getClass().getClassLoader());
        Set<ServiceLoader.ServiceDefinition> definitions = singleton(definition);
        ServiceLoader.ClassIterator<PortableHook> iterator
                = new ServiceLoader.ClassIterator<PortableHook>(definitions, PortableHook.class);

        assertFalse(iterator.hasNext());
    }

    @Test(expected = HazelcastException.class)
    public void testFailFastOnUnknownClassesFromNonHazelcastPackage() {
        ServiceLoader.ServiceDefinition definition
                = new ServiceLoader.ServiceDefinition("non.a.hazelcast.DoesNotExist", getClass().getClassLoader());
        Set<ServiceLoader.ServiceDefinition> definitions = singleton(definition);
        ServiceLoader.ClassIterator<PortableHook> iterator
                = new ServiceLoader.ClassIterator<PortableHook>(definitions, PortableHook.class);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testSkipHookLoadedByDifferentClassloader() {
        Class<?> otherInterface = newInterface(SpiDataSerializerHook.class.getName());
        ClassLoader otherClassloader = otherInterface.getClassLoader();

        Class<?> otherHook = newClassImplementingInterface("com.hazelcast.internal.serialization.DifferentHook",
                otherInterface, otherClassloader);

        //otherHook is loaded by other classloader -> it should be skipped
        ServiceLoader.ServiceDefinition definition1 = new ServiceLoader.ServiceDefinition(otherHook.getName(), otherClassloader);
        //this hook should be loaded
        ServiceLoader.ServiceDefinition definition2
                = new ServiceLoader.ServiceDefinition(SpiDataSerializerHook.class.getName(), SpiDataSerializerHook.class.getClassLoader());

        Set<ServiceLoader.ServiceDefinition> definitions = setOf(definition1, definition2);
        ServiceLoader.ClassIterator<DataSerializerHook> iterator
                = new ServiceLoader.ClassIterator<>(definitions, DataSerializerHook.class);

        assertTrue(iterator.hasNext());
        Class<DataSerializerHook> hook = iterator.next();
        assertEquals(SpiDataSerializerHook.class, hook);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testPrivatePortableHook() {
        String hookName = DummyPrivatePortableHook.class.getName();
        ClassLoader classLoader = DummyPrivatePortableHook.class.getClassLoader();
        ServiceLoader.ServiceDefinition definition = new ServiceLoader.ServiceDefinition(hookName, classLoader);

        ServiceLoader.ClassIterator<PortableHook> classIterator
                = new ServiceLoader.ClassIterator<PortableHook>(singleton(definition), PortableHook.class);
        ServiceLoader.NewInstanceIterator<PortableHook> instanceIterator
                = new ServiceLoader.NewInstanceIterator<PortableHook>(classIterator);

        assertTrue(instanceIterator.hasNext());
        DummyPrivatePortableHook hook = (DummyPrivatePortableHook) instanceIterator.next();
        assertNotNull(hook);
    }

    @Test
    public void testPrivateSerializerHook() {
        String hookName = DummyPrivateSerializerHook.class.getName();
        ClassLoader classLoader = DummyPrivateSerializerHook.class.getClassLoader();
        ServiceLoader.ServiceDefinition definition = new ServiceLoader.ServiceDefinition(hookName, classLoader);

        ServiceLoader.ClassIterator<SerializerHook> classIterator
                = new ServiceLoader.ClassIterator<SerializerHook>(singleton(definition), SerializerHook.class);
        ServiceLoader.NewInstanceIterator<SerializerHook> instanceIterator
                = new ServiceLoader.NewInstanceIterator<SerializerHook>(classIterator);

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
        Class<ServiceLoaderSpecialCharsTestInterface> type = ServiceLoaderSpecialCharsTestInterface.class;
        String factoryId = "com.hazelcast.ServiceLoaderSpecialCharsTestInterface";

        URL url = ClassLoader.getSystemResource("test with special chars^/");
        ClassLoader given = new URLClassLoader(new URL[]{url});

        Set<ServiceLoaderSpecialCharsTestInterface> implementations = new HashSet<ServiceLoaderSpecialCharsTestInterface>();
        Iterator<ServiceLoaderSpecialCharsTestInterface> iterator = ServiceLoader.iterator(type, factoryId, given);
        while (iterator.hasNext()) {
            implementations.add(iterator.next());
        }

        assertEquals(1, implementations.size());
    }

    @Test
    public void loadServicesFromInMemoryClassLoader() throws Exception {
        Class<ServiceLoaderTestInterface> type = ServiceLoaderTestInterface.class;
        String factoryId = "com.hazelcast.InMemoryFileForTesting";

        ClassLoader parent = this.getClass().getClassLoader();
        // Handles META-INF/services/com.hazelcast.CustomServiceLoaderTestInterface
        ClassLoader given = new CustomUrlStreamHandlerClassloader(parent);

        Set<ServiceLoaderTestInterface> implementations = new HashSet<ServiceLoaderTestInterface>();
        Iterator<ServiceLoaderTestInterface> iterator = ServiceLoader.iterator(type, factoryId, given);
        while (iterator.hasNext()) {
            implementations.add(iterator.next());
        }

        assertEquals(1, implementations.size());
    }

    @Test
    public void testClassIteratorInTomcat_whenClassesInBothLibs()
            throws Exception {
        ClassLoader launchClassLoader = this.getClass().getClassLoader();
        ClassLoader webappClassLoader;
        // setup embedded tomcat
        Tomcat tomcat = new Tomcat();
        tomcat.setPort(13256); // 8080 may be used by some other tests
        Context ctx = tomcat.addContext("", null);
        // Map target/classes as WEB-INF/classes, so webapp classloader
        // will locate compiled production classes in the webapp classpath.
        // The purpose of this setup is to make project classes available
        // to both launch classloader and webapplication classloader,
        // modeling a Tomcat deployment in which Hazelcast JARs are deployed
        // in both tomcat/lib and webapp/lib
        File webInfClasses = new File("target/classes");
        WebResourceRoot resources = new StandardRoot(ctx);
        resources.addPreResources(new DirResourceSet(resources, "/WEB-INF/classes",
                webInfClasses.getAbsolutePath(), "/"));
        ctx.setResources(resources);

        TestServiceLoaderServlet testServlet = new TestServiceLoaderServlet();
        Wrapper wrapper = tomcat.addServlet("", "testServlet", testServlet);
        wrapper.setLoadOnStartup(1);
        ctx.addServletMappingDecoded("/", "testServlet");

        tomcat.start();
        try {
            assertTrueEventually(() -> assertTrue(testServlet.isInitDone()));
            assertNull("No failure is expected from servlet init() method", testServlet.failure());

            webappClassLoader = testServlet.getWebappClassLoader();

            assertNotEquals(launchClassLoader, webappClassLoader);
            Iterator<? extends Class<?>> iterator
                    = ServiceLoader.classIterator(DataSerializerHook.class, "com.hazelcast.DataSerializerHook",
                    webappClassLoader);
            assertTrue(iterator.hasNext());
            while (iterator.hasNext()) {
                Class<?> klass = iterator.next();
                assertEquals(launchClassLoader, klass.getClassLoader());
            }
        } finally {
            tomcat.stop();
        }
    }

    private static class TestServiceLoaderServlet extends HttpServlet {

        private static final long serialVersionUID = 1L;
        private volatile ClassLoader webappClassLoader;
        private AtomicBoolean initDone = new AtomicBoolean();
        private AtomicReference<Throwable> failure = new AtomicReference<>();

        @Override
        public void init() throws ServletException {
            super.init();
            try {
                webappClassLoader = Thread.currentThread().getContextClassLoader();
                Class<?> dshKlass = webappClassLoader.loadClass(DataSerializerHook.class.getName());
                Iterator<? extends Class<?>> iterator
                        = ServiceLoader.classIterator(dshKlass, "com.hazelcast.DataSerializerHook",
                        webappClassLoader);
                // webapp classloader locates and loads classes from webapp classpath
                assertTrue(iterator.hasNext());
                while (iterator.hasNext()) {
                    Class<?> klass = iterator.next();
                    assertEquals(webappClassLoader, klass.getClassLoader());
                }
            } catch (Throwable t) {
                failure.set(t);
                throw new ServletException(t);
            } finally {
                initDone.set(true);
            }
        }

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp)
                throws IOException {
            resp.setContentType("text/plain");
            resp.getWriter().print("OK");
        }

        public ClassLoader getWebappClassLoader() {
            return webappClassLoader;
        }

        public boolean isInitDone() {
            return initDone.get();
        }

        public Throwable failure() {
            return failure.get();
        }
    }

    public interface ServiceLoaderTestInterface {
    }

    public interface ServiceLoaderSpecialCharsTestInterface {
    }

    public static class ServiceLoaderTestInterfaceImpl implements ServiceLoaderTestInterface {
    }

    public static class ServiceLoaderSpecialCharsTestInterfaceImpl implements ServiceLoaderSpecialCharsTestInterface {
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

    /**
     * Delegates everything to a given parent classloader.
     * When a loaded class is defined by the parent then it "steals"
     * its bytecode and try to define it on its own.
     * <p>
     * It simulates the situation where child and parent defines the same classes.
     */
    private static class StealingClassloader extends ClassLoader {
        private final ClassLoader parent;

        private StealingClassloader(ClassLoader parent) {
            super(parent);
            this.parent = parent;
        }

        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException {
            Class<?> loadedByParent = parent.loadClass(name);
            if (loadedByParent != null && parent.equals(loadedByParent.getClassLoader())) {
                byte[] bytecode = loadBytecodeFromParent(name);
                Class<?> clazz = defineClass(name, bytecode, 0, bytecode.length);
                resolveClass(clazz);
                return clazz;
            } else {
                return loadedByParent;
            }
        }

        @Override
        public Enumeration<URL> getResources(String name) throws IOException {
            return parent.getResources(name);
        }

        @Override
        public InputStream getResourceAsStream(String name) {
            return parent.getResourceAsStream(name);
        }

        private byte[] loadBytecodeFromParent(String className) {
            String resource = className.replace('.', '/').concat(".class");
            InputStream is = null;
            try {
                is = parent.getResourceAsStream(resource);
                if (is != null) {
                    try {
                        return toByteArray(is);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            } finally {
                IOUtil.closeResource(is);
            }
            return null;
        }

    }

    /**
     * Delegates everything to a given parent classloader, except for a single
     * file.
     */
    private static class CustomUrlStreamHandlerClassloader extends ClassLoader {
        private final String inMemoryResourceName = "META-INF/services/com.hazelcast.InMemoryFileForTesting";
        private final String inMemoryContent = "com.hazelcast.internal.util.ServiceLoaderTest$ServiceLoaderTestInterfaceImpl";

        private CustomUrlStreamHandlerClassloader(ClassLoader parent) {
            super(parent);
        }

        @Override
        public URL findResource(String name) {
            if (!inMemoryResourceName.equals(name)) {
                return null;
            }
            try {
                return new URL(null, "inmemory:" + name, new URLStreamHandler() {
                    @Override
                    protected URLConnection openConnection(URL u) {
                        return new URLConnection(u) {
                            private final ByteArrayInputStream in = new ByteArrayInputStream(inMemoryContent.getBytes());

                            @Override
                            public void connect() {
                            }

                            @Override
                            public InputStream getInputStream() {
                                return in;
                            }
                        };
                    }
                });
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Enumeration<URL> findResources(String name) {
            URL resource = findResource(name);
            return resource == null ? Collections.emptyEnumeration() : Collections.enumeration(Arrays.asList(resource));
        }
    }
}
