package com.hazelcast.query.impl.extraction;

import com.hazelcast.config.InMemoryFormat;

public class SingleCollectionExtractorExtractionTest extends SingleCollectionReflectionExtractionTest {

    public SingleCollectionExtractorExtractionTest(InMemoryFormat inMemoryFormat, Index index) {
        super(inMemoryFormat, index);
    }

    protected Configurator getConfigurator() {
        return new SingleCollectionDataStructure.LimbExtractorsConfigurator();
    }


}
