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

package com.hazelcast.sql.impl.schema.map;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.sample.MapSampleMetadata;
import com.hazelcast.sql.impl.schema.map.sample.MapSampleMetadataResolver;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ResolverUtils {

    private ResolverUtils() { }

    @Nullable
    public static ResolveResult resolvePartitionedMap(InternalSerializationService ss, MapServiceContext context, String name) {
        MapContainer mapContainer = context.getMapContainer(name);

        // Handle concurrent map destroy.
        if (mapContainer == null) {
            return null;
        }

        MapConfig config = mapContainer.getMapConfig();

        // HD maps are not supported at the moment.
        if (config.getInMemoryFormat() == InMemoryFormat.NATIVE) {
            throw QueryException.error("IMap with InMemoryFormat.NATIVE is not supported: " + name);
        }

        boolean binary = config.getInMemoryFormat() == InMemoryFormat.BINARY;

        for (PartitionContainer partitionContainer : context.getPartitionContainers()) {
            // Resolve sample.
            RecordStore<?> recordStore = partitionContainer.getExistingRecordStore(name);

            if (recordStore == null) {
                continue;
            }

            Iterator<Map.Entry<Data, Record>> recordStoreIterator = recordStore.iterator();

            if (!recordStoreIterator.hasNext()) {
                continue;
            }

            Map.Entry<Data, Record> entry = recordStoreIterator.next();

            MapSampleMetadata keyMetadata = MapSampleMetadataResolver.resolve(
                    ss,
                    entry.getKey(),
                    binary,
                    true
            );

            MapSampleMetadata valueMetadata = MapSampleMetadataResolver.resolve(
                    ss,
                    entry.getValue().getValue(),
                    binary,
                    false
            );

            return new ResolveResult(
                    mergeMapFields(keyMetadata.getFields(), valueMetadata.getFields()),
                    keyMetadata.getDescriptor(),
                    valueMetadata.getDescriptor());
        }

        // no sample entry found on local member
        return null;
    }

    private static List<TableField> mergeMapFields(Map<String, TableField> keyFields, Map<String, TableField> valueFields) {
        LinkedHashMap<String, TableField> res = new LinkedHashMap<>(keyFields);

        for (Map.Entry<String, TableField> valueFieldEntry : valueFields.entrySet()) {
            // Value fields do not override key fields.
            res.putIfAbsent(valueFieldEntry.getKey(), valueFieldEntry.getValue());
        }

        return new ArrayList<>(res.values());
    }

    public static final class ResolveResult {
        private final List<TableField> fields;
        private final QueryTargetDescriptor keyDescriptor;
        private final QueryTargetDescriptor valueDescriptor;

        public ResolveResult(
                List<TableField> fields,
                QueryTargetDescriptor keyDescriptor,
                QueryTargetDescriptor valueDescriptor
        ) {
            this.fields = fields;
            this.keyDescriptor = keyDescriptor;
            this.valueDescriptor = valueDescriptor;
        }

        public List<TableField> getFields() {
            return fields;
        }

        public QueryTargetDescriptor getKeyDescriptor() {
            return keyDescriptor;
        }

        public QueryTargetDescriptor getValueDescriptor() {
            return valueDescriptor;
        }
    }
}
