package com.hazelcast.query.impl;

import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.query.extractor.ValueExtractor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Extractors {

    private static final Extractors EMPTY = new Extractors(Collections.<MapAttributeConfig>emptyList());

    private final Map<String, ValueExtractor> extractors;

    public Extractors(List<MapAttributeConfig> mapAttributeConfigs) {
        extractors = new HashMap<String, ValueExtractor>();
        for (MapAttributeConfig config : mapAttributeConfigs) {
            if (extractors.containsKey(config.getName())) {
                throw new IllegalArgumentException("Could not add " + config + ". Extractor for this attribute name already added.");
            }
            extractors.put(config.getName(), instantiateExtractor(config));
        }
    }

    private ValueExtractor instantiateExtractor(MapAttributeConfig config) {
        try {
            Class<?> clazz = Class.forName(config.getExtractor());
            Object extractor = clazz.newInstance();
            if (extractor instanceof ValueExtractor) {
                return (ValueExtractor) extractor;
            } else {
                throw new IllegalArgumentException("Extractor does not extend ValueExtractor class " + config);
            }
        } catch (IllegalAccessException ex) {
            throw new RuntimeException("Could not initialize extractor " + config, ex);
        } catch (InstantiationException ex) {
            throw new RuntimeException("Could not initialize extractor " + config, ex);
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException("Could not initialize extractor " + config, ex);
        }
    }

    public ValueExtractor getExtractor(String attribute) {
        return extractors.get(attribute);
    }

    public static Extractors empty() {
        return EMPTY;
    }

}
