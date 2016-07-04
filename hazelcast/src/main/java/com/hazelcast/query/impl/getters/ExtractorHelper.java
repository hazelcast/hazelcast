/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.query.impl.getters;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.util.StringUtil;

public final class ExtractorHelper {

    private ExtractorHelper() {
    }

	static Map<String, ValueExtractor> instantiateExtractors(final Config config,
			List<MapAttributeConfig> mapAttributeConfigs) {
        Map<String, ValueExtractor> extractors = new HashMap<String, ValueExtractor>();
		for (MapAttributeConfig attrConfig : mapAttributeConfigs) {
			if (extractors.containsKey(attrConfig.getName())) {
                throw new IllegalArgumentException("Could not add " + config
                        + ". Extractor for this attribute name already added.");
            }
			extractors.put(attrConfig.getName(), instantiateExtractor(config, attrConfig));
        }
        return extractors;
    }
	
	static ValueExtractor instantiateExtractor(final Config config, MapAttributeConfig attrConfig) {
		
		ValueExtractor extractor = null;
		
		// Let's try with Config classLoader 
		if (config != null) {
			try {
				extractor = instantiateExtractorFromConfigClassLoader(config, attrConfig);
			} catch (IllegalArgumentException ex) {
				// Do nothing, Just try another way to load the Extractor
			}
		}
		
		// Lets's try with static classLoader
		if (extractor == null) {
			extractor = instantiateExtractor(attrConfig);
		}
		
		return extractor;
	}

    static ValueExtractor instantiateExtractor(MapAttributeConfig config) {
        try {
            Class<?> clazz = Class.forName(config.getExtractor());
            Object extractor = clazz.newInstance();
            if (extractor instanceof ValueExtractor) {
                return (ValueExtractor) extractor;
            } else {
                throw new IllegalArgumentException("Extractor does not extend ValueExtractor class " + config);
            }
        } catch (IllegalAccessException ex) {
            throw new IllegalArgumentException("Could not initialize extractor " + config, ex);
        } catch (InstantiationException ex) {
            throw new IllegalArgumentException("Could not initialize extractor " + config, ex);
        } catch (ClassNotFoundException ex) {
            throw new IllegalArgumentException("Could not initialize extractor " + config, ex);
        }
    }
	
	static ValueExtractor instantiateExtractorFromConfigClassLoader(final Config config,
			MapAttributeConfig attrConfig) {
		
		try {
			ClassLoader cLoader = config.getClassLoader();
			if (cLoader == null) {
				throw new IllegalArgumentException("Could not initialize extractor. Config classLoader not present");
			}
			Class<?> clazz = cLoader.loadClass(attrConfig.getExtractor());
			Object extractor = clazz.newInstance();
			if (extractor instanceof ValueExtractor) {
				return (ValueExtractor) extractor;
			} else {
				throw new IllegalArgumentException("Extractor does not extend ValueExtractor class " + config);
			}
		} catch (IllegalAccessException ex) {
			throw new IllegalArgumentException("Could not initialize extractor " + config, ex);
		} catch (InstantiationException ex) {
			throw new IllegalArgumentException("Could not initialize extractor " + config, ex);
		} catch (ClassNotFoundException ex) {
			throw new IllegalArgumentException("Could not initialize extractor " + config, ex);
		}
	}

    public static String extractAttributeNameNameWithoutArguments(String attributeNameWithArguments) {
        int start = StringUtil.lastIndexOf(attributeNameWithArguments, '[');
        int end = StringUtil.lastIndexOf(attributeNameWithArguments, ']');
        if (start > 0 && end > 0 && end > start) {
            return attributeNameWithArguments.substring(0, start);
        }
        if (start < 0 && end < 0) {
            return attributeNameWithArguments;
        }
        throw new IllegalArgumentException("Wrong argument input passed " + attributeNameWithArguments);

    }

    public static String extractArgumentsFromAttributeName(String attributeNameWithArguments) {
        int start = StringUtil.lastIndexOf(attributeNameWithArguments, '[');
        int end = StringUtil.lastIndexOf(attributeNameWithArguments, ']');
        if (start > 0 && end > 0 && end > start) {
            return attributeNameWithArguments.substring(start + 1, end);
        }
        if (start < 0 && end < 0) {
            return null;
        }
        throw new IllegalArgumentException("Wrong argument input passed " + attributeNameWithArguments);
    }

}
