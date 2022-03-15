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

package com.hazelcast.internal.config;

import com.hazelcast.config.ConfigRecognizer;
import com.hazelcast.config.ConfigStream;

import java.util.Collection;

/**
 * Abstract composite {@link ConfigRecognizer} implementation that uses
 * multiple recognizers under the hood.
 */
public class AbstractConfigRecognizer implements ConfigRecognizer {
    protected final Collection<ConfigRecognizer> recognizers;

    public AbstractConfigRecognizer(Collection<ConfigRecognizer> recognizers) {
        this.recognizers = recognizers;
    }

    @Override
    public boolean isRecognized(ConfigStream configStream) throws Exception {
        boolean recognized = false;
        for (ConfigRecognizer recognizer : recognizers) {
            configStream.reset();
            recognized = recognized || recognizer.isRecognized(configStream);
        }

        return recognized;
    }
}
