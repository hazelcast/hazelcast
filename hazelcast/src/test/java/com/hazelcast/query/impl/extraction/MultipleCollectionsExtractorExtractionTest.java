package com.hazelcast.query.impl.extraction;

import com.hazelcast.config.InMemoryFormat;

import static com.hazelcast.query.impl.extraction.MultipleCollectionsDataStructure.FingerExtractorsConfigurator;

public class MultipleCollectionsExtractorExtractionTest extends MultipleCollectionsReflectionExtractionTest {

    public MultipleCollectionsExtractorExtractionTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
        super(inMemoryFormat, index, null);
    }

    protected Configurator getConfigurator() {
        return new FingerExtractorsConfigurator();
    }


}
