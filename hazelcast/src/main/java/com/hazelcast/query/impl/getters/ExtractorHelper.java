/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.AttributeConfig;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.logging.Logger;
import com.hazelcast.query.extractor.ValueExtractor;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.hazelcast.internal.util.MapUtil.createHashMap;

public final class ExtractorHelper {

    private ExtractorHelper() {
    }

    static Map<String, ValueExtractor> instantiateExtractors(List<AttributeConfig> attributeConfigs,
                                                             ClassLoader classLoader) {
        Map<String, ValueExtractor> extractors = createHashMap(attributeConfigs.size());
        for (AttributeConfig config : attributeConfigs) {
            if (extractors.containsKey(config.getName())) {
                throw new IllegalArgumentException("Could not add " + config
                        + ". Extractor for this attribute name already added.");
            }
            extractors.put(config.getName(), instantiateExtractor(config, classLoader));
        }
        return extractors;
    }

    static ValueExtractor instantiateExtractor(AttributeConfig config, ClassLoader classLoader) {
        ValueExtractor extractor = null;
        if (classLoader != null) {
            try {
                extractor = instantiateExtractorWithConfigClassLoader(config, classLoader);
            } catch (IllegalArgumentException ex) {
                // cached back-stage, initialised lazily since it's not a common case
                Logger.getLogger(ExtractorHelper.class)
                        .warning("Could not instantiate extractor with the config class loader", ex);
            }
        }

        if (extractor == null) {
            extractor = instantiateExtractorWithClassForName(config);
        }
        return extractor;
    }

    private static ValueExtractor instantiateExtractorWithConfigClassLoader(AttributeConfig config, ClassLoader classLoader) {
        try {
            Class<?> clazz = classLoader.loadClass(config.getExtractorClassName());
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

    private static ValueExtractor instantiateExtractorWithClassForName(AttributeConfig config) {
        try {
            Class<?> clazz = Class.forName(config.getExtractorClassName());
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

    /**
     * @param add            Consumer that primitives will be passed to
     * @param primitiveArray primitive array to read from
     * @return false if primitive array is empty
     */
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:methodlength", "checkstyle:returncount"})
    public static boolean reducePrimitiveArrayInto(Consumer add, Object primitiveArray) {
        // XXX: Standard Array.get has really bad performance, see
        // https://bugs.openjdk.java.net/browse/JDK-8051447. For large arrays
        // it may consume significant amount of time, so we are doing the
        // reduction manually for each primitive type.

        if (primitiveArray instanceof long[]) {
            long[] array = (long[]) primitiveArray;
            if (array.length == 0) {
                return false;
            } else {
                for (long value : array) {
                    add.accept(value);
                }
            }
        } else if (primitiveArray instanceof int[]) {
            int[] array = (int[]) primitiveArray;
            if (array.length == 0) {
                return false;
            } else {
                for (int value : array) {
                    add.accept(value);
                }
            }
        } else if (primitiveArray instanceof short[]) {
            short[] array = (short[]) primitiveArray;
            if (array.length == 0) {
                return false;
            } else {
                for (short value : array) {
                    add.accept(value);
                }
            }
        } else if (primitiveArray instanceof byte[]) {
            byte[] array = (byte[]) primitiveArray;
            if (array.length == 0) {
                return false;
            } else {
                for (byte value : array) {
                    add.accept(value);
                }
            }
        } else if (primitiveArray instanceof char[]) {
            char[] array = (char[]) primitiveArray;
            if (array.length == 0) {
                return false;
            } else {
                for (char value : array) {
                    add.accept(value);
                }
            }
        } else if (primitiveArray instanceof boolean[]) {
            boolean[] array = (boolean[]) primitiveArray;
            if (array.length == 0) {
                return false;
            } else {
                for (boolean value : array) {
                    add.accept(value);
                }
            }
        } else if (primitiveArray instanceof double[]) {
            double[] array = (double[]) primitiveArray;
            if (array.length == 0) {
                return false;
            } else {
                for (double value : array) {
                    add.accept(value);
                }
            }

        } else if (primitiveArray instanceof float[]) {
            float[] array = (float[]) primitiveArray;
            if (array.length == 0) {
                return false;
            } else {
                for (float value : array) {
                    add.accept(value);
                }
            }
        } else {
            throw new IllegalArgumentException("unexpected primitive array: " + primitiveArray);
        }
        return true;
    }
}
