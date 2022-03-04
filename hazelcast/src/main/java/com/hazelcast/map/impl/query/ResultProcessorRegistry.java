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

package com.hazelcast.map.impl.query;

import java.util.HashMap;
import java.util.Map;

/**
 * Holds a registry of ResultProcessors for Result type. Enables running the post-processing in a generic way.
 */
public class ResultProcessorRegistry {

    private final Map<Class<? extends Result>, ResultProcessor> processors
            = new HashMap<>();

    public void registerProcessor(Class<? extends Result> clazz, ResultProcessor processor) {
        processors.put(clazz, processor);
    }

    public ResultProcessor get(Class<? extends Result> clazz) {
        return processors.get(clazz);
    }
}
