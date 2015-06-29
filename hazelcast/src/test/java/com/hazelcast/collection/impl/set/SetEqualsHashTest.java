package com.hazelcast.collection.impl.set;

import com.hazelcast.collection.impl.collection.CollectionItem;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SetEqualsHashTest extends HazelcastTestSupport {

    @Test
    public void testCollectionItem_equalsAndHash(){
        SerializationServiceBuilder serializationServiceBuilder = new DefaultSerializationServiceBuilder();
        SerializationService build = serializationServiceBuilder.build();
        Data value = build.toData(randomString());
        CollectionItem firstItem = new CollectionItem(1, value);
        CollectionItem secondItem = new CollectionItem(2, value);
        assertTrue(firstItem.equals(secondItem));
        assertEquals(firstItem.hashCode(), secondItem.hashCode());
    }
}
