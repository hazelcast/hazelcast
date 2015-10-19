package com.hazelcast.query.impl.extraction;

import com.hazelcast.config.InMemoryFormat;

public class SingleCollectionExtractorExtractionTest extends SingleCollectionReflectionExtractionTest {

    public SingleCollectionExtractorExtractionTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
        super(inMemoryFormat, index, null);
    }

    protected Configurator getConfigurator() {
        return new SingleCollectionDataStructure.LimbExtractorsConfigurator();
    }


}
