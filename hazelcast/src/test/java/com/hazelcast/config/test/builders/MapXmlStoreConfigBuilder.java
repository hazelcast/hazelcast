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

package com.hazelcast.config.test.builders;

import com.hazelcast.config.MapStoreConfig;

public class MapXmlStoreConfigBuilder {
    private boolean enabled;
    private MapStoreConfig.InitialLoadMode initialMode;
    private String className;
    private int writeDelaySeconds;
    private int writeBatchSize;

    public MapXmlStoreConfigBuilder enabled() {
        this.enabled = true;
        return this;
    }

    public MapXmlStoreConfigBuilder disabled() {
        this.enabled = false;
        return this;
    }

    public MapXmlStoreConfigBuilder withInitialMode(MapStoreConfig.InitialLoadMode initialMode) {
        this.initialMode = initialMode;
        return this;
    }

    public MapXmlStoreConfigBuilder withClassName(String className) {
        this.className = className;
        return this;
    }

    public MapXmlStoreConfigBuilder withWriteDelay(int writeDelaySeconds) {
        this.writeDelaySeconds = writeDelaySeconds;
        return this;
    }

    public MapXmlStoreConfigBuilder withWriteBatchSize(int writeBatchSize) {
        this.writeBatchSize = writeBatchSize;
        return this;
    }

    public String build() {
        return "<map-store enabled=\"" + enabled + "\" initial-mode=\"" + initialMode + "\">\n"
                + "    <class-name>" + className + "</class-name>\n"
                + "    <write-delay-seconds>" + writeDelaySeconds + "</write-delay-seconds>\n"
                + "    <write-batch-size>" + writeBatchSize + "</write-batch-size>\n"
                + "</map-store>";
    }
}
