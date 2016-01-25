package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapAttributeConfigReadOnlyTest {

    @Test(expected = UnsupportedOperationException.class)
    public void testSetName() throws Exception {
        new MapAttributeConfigReadOnly(mock(MapAttributeConfig.class)).setName("extractedName");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetExtractor() throws Exception {
        new MapAttributeConfigReadOnly(mock(MapAttributeConfig.class)).setExtractor("com.test.Extractor");
    }

}