package com.hazelcast.internal.distributedclassloading.impl;

import com.hazelcast.config.DistributedClassloadingConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.config.DistributedClassloadingConfig.ProviderMode.LOCAL_AND_CACHED_CLASSES;
import static com.hazelcast.config.DistributedClassloadingConfig.ProviderMode.LOCAL_CLASSES_ONLY;
import static com.hazelcast.config.DistributedClassloadingConfig.ProviderMode.OFF;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClassDataProviderTest {

    @Test
    public void givenProviderModeSetToOFF_whenMapClassContainsClass_thenReturnNull() throws Exception {
        DistributedClassloadingConfig.ProviderMode providerMode = OFF;
        String className = "className";
        ClassSource classSource = newMockClassSource(className);
        ClassLoader parent = getClass().getClassLoader();
        ClassDataProvider provider = createClassDataProvider(providerMode, className, classSource, parent);
        ClassData classData = provider.getClassDataOrNull(className);
        assertNull(classData);
    }

    @Test
    public void givenProviderModeSetToLOCAL_CLASSES_ONLY_whenMapClassContainsClass_thenReturnNull() throws Exception {
        DistributedClassloadingConfig.ProviderMode providerMode = LOCAL_CLASSES_ONLY;
        String className = "className";
        ClassSource classSource = newMockClassSource(className);
        ClassLoader parent = getClass().getClassLoader();
        ClassDataProvider provider = createClassDataProvider(providerMode, className, classSource, parent);
        ClassData classData = provider.getClassDataOrNull(className);
        assertNull(classData);
    }

    @Test
    public void givenProviderModeSetToLOCAL_AND_CACHED_CLASSES_whenMapClassContainsClass_thenReturnIt() throws Exception {
        DistributedClassloadingConfig.ProviderMode providerMode = LOCAL_AND_CACHED_CLASSES;
        String className = "className";
        ClassSource classSource = newMockClassSource(className);
        ClassLoader parent = getClass().getClassLoader();
        ClassDataProvider provider = createClassDataProvider(providerMode, className, classSource, parent);
        ClassData classData = provider.getClassDataOrNull(className);
        assertNotNull(classData.getClassDefinition());
    }


    private ClassDataProvider createClassDataProvider(DistributedClassloadingConfig.ProviderMode providerMode,
                                                      String className, ClassSource classSource, ClassLoader parent) {
        ILogger logger = mock(ILogger.class);
        ConcurrentMap<String, ClassSource> classSourceMap = new ConcurrentHashMap<String, ClassSource>();
        classSourceMap.put(className, classSource);
        return new ClassDataProvider(providerMode, parent, classSourceMap, logger);
    }

    private static ClassSource newMockClassSource(String classname) {
        return new ClassSource(classname, new byte[0], null, null);
    }

}