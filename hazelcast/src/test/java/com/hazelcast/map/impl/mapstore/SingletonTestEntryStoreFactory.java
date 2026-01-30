/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.map.EntryLoader;
import com.hazelcast.map.MapLoader;
import com.hazelcast.map.MapStoreFactory;

import java.util.Properties;

public class SingletonTestEntryStoreFactory implements MapStoreFactory<String, EntryLoader.MetadataAwareValue<String>> {

    public static final TestEntryStore<String, String> INSTANCE = new TestEntryStore<>();

    @Override
    public MapLoader<String, EntryLoader.MetadataAwareValue<String>> newMapStore(String mapName, Properties properties) {
        return INSTANCE;
    }

    public static void assertRecordStored(String key, String value) {
        INSTANCE.assertRecordStored(key, value);
    }

    public static void assertRecordStored(String key, String value, long expectedExpirationTime, long delta) {
        INSTANCE.assertRecordStored(key, value, expectedExpirationTime, delta);
    }

    public void putExternally(String key, String value, long expirationTime) {
        INSTANCE.putExternally(key, value, expirationTime);
    }

    public void putExternally(String key, String value) {
        INSTANCE.putExternally(key, value);
    }
}
