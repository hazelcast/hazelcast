/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

public class MapXmlConfigBuilder {
    private static final int DEFAULT_BACKUP_COUNT = 1;

    private String name;
    private int backupCount = DEFAULT_BACKUP_COUNT;
    private int timeToLiveSeconds;
    private MapXmlStoreConfigBuilder mapXmlStoreConfigBuilder;

    public MapXmlConfigBuilder withName(String name) {
        this.name = name;
        return this;
    }

    public MapXmlConfigBuilder withBackupCount(int backupCount) {
        this.backupCount = backupCount;
        return this;
    }

    public MapXmlConfigBuilder withTimeToLive(int timeToLiveSeconds) {
        this.timeToLiveSeconds = timeToLiveSeconds;
        return this;
    }

    public MapXmlConfigBuilder withStore(MapXmlStoreConfigBuilder mapXmlStoreConfigBuilder) {
        this.mapXmlStoreConfigBuilder = mapXmlStoreConfigBuilder;
        return this;
    }

    public String build() {
        return "<map name=\"" + name + "\">\n"
                + "<backup-count>" + backupCount + "</backup-count>"
                + "<time-to-live-seconds>" + timeToLiveSeconds + "</time-to-live-seconds>"
                + (mapXmlStoreConfigBuilder != null ? mapXmlStoreConfigBuilder.build() : "")
                + "</map>\n";
    }
}
