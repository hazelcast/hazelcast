/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.nio.serialization.Data;

public class MapRecordKey {
    final String mapName;
    final Data key;

    public MapRecordKey(String mapName, Data key) {
        this.mapName = mapName;
        this.key = key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MapRecordKey that = (MapRecordKey) o;

        if (key != null ? !key.equals(that.key) : that.key != null) {
            return false;
        }
        if (mapName != null ? !mapName.equals(that.mapName) : that.mapName != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = mapName != null ? mapName.hashCode() : 0;
        result = 31 * result + (key != null ? key.hashCode() : 0);
        return result;
    }
}
