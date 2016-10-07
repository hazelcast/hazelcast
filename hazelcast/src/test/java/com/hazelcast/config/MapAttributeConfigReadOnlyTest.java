package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapAttributeConfigReadOnlyTest {

    private MapAttributeConfig getReadOnlyConfig() {
        return new MapAttributeConfig().getAsReadOnly();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setNameOfReadOnlyMapAttributeConfigShouldFail() throws Exception {
        getReadOnlyConfig().setName("extractedName");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setExtractorOfReadOnlyMapAttributeConfigShouldFail() throws Exception {
        getReadOnlyConfig().setExtractor("com.test.Extractor");
    }
}
