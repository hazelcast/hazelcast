package com.hazelcast.query.impl.extraction;

import com.hazelcast.config.InMemoryFormat;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.hazelcast.query.impl.extraction.SingleValueDataStructure.BrainExtractorsConfigurator;


@RunWith(Parameterized.class)
public class SingleValueExtractorExtractionTest extends SingleValueReflectionExtractionTest {

    public SingleValueExtractorExtractionTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
        super(inMemoryFormat, index, null);
    }

    protected Configurator getConfigurator() {
        return new BrainExtractorsConfigurator();
    }

}
