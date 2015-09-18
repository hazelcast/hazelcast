package com.hazelcast.internal.serialization.impl;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.serialization.InputOutputFactory;
import com.hazelcast.internal.serialization.impl.bufferpool.BufferPoolFactory;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertSame;

@RunWith(MockitoJUnitRunner.class)
@Category(QuickTest.class)
public class SerializationServiceImplTest {

    public static class RecursiveStructure implements Portable {
        private String name;
        private RecursiveStructure child;
        private RecursiveStructure[] children;

        public String getName() {
            return this.name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public RecursiveStructure getChild() {
            return this.child;
        }

        public void setChild(RecursiveStructure child) {
            this.child = child;
        }

        public RecursiveStructure[] getChildren() {
            return children;
        }

        public void setChildren(RecursiveStructure[] children) {
            this.children = children;
        }

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 1;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            throw new UnsupportedOperationException("Not needed for the test");
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            throw new UnsupportedOperationException("Not needed for the test");
        }
    }

    @Mock
    InputOutputFactory inputOutputFactory;

    int version = 1;
    ClassLoader classLoader = getClass().getClassLoader();
    Map<Integer, ? extends DataSerializableFactory> dataSerializableFactories = Collections.emptyMap();
    Map<Integer, ? extends PortableFactory> portableFactories = Collections.emptyMap();

    Collection<ClassDefinition> classDefinitions = Collections.emptyList();

    boolean checkClassDefErrors = true;

    @Mock
    ManagedContext managedContext;

    @Mock
    PartitioningStrategy partitionStrategy;

    int initialOutputBufferSize;
    boolean enableCompression = false;
    boolean enableSharedObject = false;

    @Mock
    BufferPoolFactory bufferPoolFactory;

    @Test
    public void testSerializationShouldSupportRecursiveStructures() throws Exception {
        // Here we use a fake definition having the same factory and class id as the real definition to
        // be able to pass it to addPortableXX. There methods only use these two fields of the class definition
        ClassDefinition fake = new ClassDefinitionBuilder(1, 1).build();
        ClassDefinition cd = new ClassDefinitionBuilder(1, 1)
                .addUTFField("name")
                .addPortableField("child", fake)
                .addPortableArrayField("children", fake)
                .build();
        classDefinitions = Collections.singleton(cd);

        // We get a stack overflow if it doesn't work
        SerializationServiceImpl service = new SerializationServiceImpl(
                inputOutputFactory, version, classLoader,
                dataSerializableFactories, portableFactories,
                classDefinitions, checkClassDefErrors,
                managedContext, partitionStrategy, initialOutputBufferSize,
                enableCompression, enableSharedObject,
                bufferPoolFactory
        );

        assertSame(cd, service.getPortableContext().lookupClassDefinition(1, 1, 1));
    }
}