/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.MetadataInitializer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.query.impl.JsonMetadata;
import com.hazelcast.query.impl.Metadata;

import javax.annotation.Nonnull;
import java.io.IOException;

import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

/**
 * Creates json related metadata for map records. Metadata is
 * created and set for put and update operations. There is no
 * need for removing metadata on remove events because metadata
 * is stored inside record. It is removed with the record.
 */
public class JsonMetadataMutationObserver implements MutationObserver<Record> {

    private final SerializationService serializationService;
    private final MetadataInitializer metadataInitializer;
    private final JsonMetadataStore metadataStore;
    private final InMemoryFormat inMemoryFormat;

    public JsonMetadataMutationObserver(SerializationService serializationService,
                                        MetadataInitializer metadataInitializer,
                                        JsonMetadataStore metadataStore,
                                        InMemoryFormat inMemoryFormat) {
        this.serializationService = serializationService;
        this.metadataInitializer = metadataInitializer;
        this.metadataStore = metadataStore;
        this.inMemoryFormat = inMemoryFormat;
    }

    @Override
    public void onPutRecord(@Nonnull Data key, Record record, Object oldValue, boolean backup) {
        onPutInternal(key, record);
    }

    @Override
    public void onReplicationPutRecord(@Nonnull Data key, @Nonnull Record record, boolean populateIndex) {
        onPutInternal(key, record);
    }

    @Override
    public void onUpdateRecord(@Nonnull Data key, @Nonnull Record record,
                               Object oldValue, Object newValue, boolean backup) {
        updateValueMetadataIfNecessary(key, newValue);
    }

    @Override
    public void onLoadRecord(@Nonnull Data key, @Nonnull Record record, boolean backup) {
        onPutInternal(key, record);
    }

    @Override
    public void onRemoveRecord(Data key, Record record, boolean backup) {
        metadataStore.remove(key);
    }

    @Override
    public void onEvictRecord(Data key, Record record, boolean backup) {
        metadataStore.remove(key);
    }

    @Override
    public void onReset() {
        metadataStore.clear();
    }

    @Override
    public void onClear() {
        metadataStore.clear();
    }

    @Override
    public void onDestroy(boolean isDuringShutdown, boolean internal) {
        metadataStore.clear();
    }

    protected JsonMetadata getMetadata(Data dataKey) {
        return metadataStore.get(dataKey);
    }

    protected void setMetadata(Data dataKey, JsonMetadata metadata) {
        metadataStore.set(dataKey, metadata);
    }

    protected void setMetadataValue(Data dataKey, Object metadataValue) {
        metadataStore.setValue(dataKey, metadataValue);
    }

    protected void removeMetadata(Data dataKey) {
        metadataStore.remove(dataKey);
    }

    private void onPutInternal(Data dataKey, Record record) {
        Metadata metadata = initializeMetadata(dataKey, record.getValue());
        if (metadata != null) {
            setMetadata(dataKey, metadata);
        }
    }

    private void updateValueMetadataIfNecessary(Data dataKey,
                                                Object updateValue) {
        Object valueMetadata = null;
        try {
            // in some cases (eg. entry processors) observers are invoked before new value is serialized
            // but the JSON metadata must agree with final form of the data stored in the IMap
            // (see JsonSchemaHelper.getTokenLocation)
            valueMetadata = switch (inMemoryFormat) {
                case OBJECT -> {
                    // deserialize only JSON, other types would be ignored anyway, so we can pass Data
                    var valueObject = updateValue instanceof Data serializedData && serializedData.isJson()
                            ? serializationService.toObject(updateValue)
                            : updateValue;
                    yield metadataInitializer.createFromObject(valueObject);
                }

                case BINARY, NATIVE -> {
                    if (updateValue instanceof Data valueData) {
                        yield metadataInitializer.createFromData(valueData);
                    } else if (updateValue instanceof HazelcastJsonValue) {
                        // serialize only HazelcastJsonValue, other types would be ignored
                        yield metadataInitializer.createFromData(serializationService.toData(updateValue));
                    } else {
                        yield null;
                    }
                }
            };
        } catch (IOException e) {
            // silently ignore exception. Json string is allowed to be invalid.
            ignore(e);
        } catch (Exception e) {
            throw rethrow(e);
        }

        setMetadataValue(dataKey, valueMetadata);
    }

    private Metadata initializeMetadata(Data key, Object value) {
        try {
            Object keyMetadata = metadataInitializer.createFromData(key);
            Object valueMetadata;
            if (value instanceof Data data) {
                valueMetadata = metadataInitializer.createFromData(data);
            } else {
                valueMetadata = metadataInitializer.createFromObject(value);
            }
            if (keyMetadata != null || valueMetadata != null) {
                Metadata metadata = new Metadata(keyMetadata, valueMetadata);
                return metadata;
            }
            return null;
        } catch (IOException e) {
            // silently ignore exception. Json string is allowed to be invalid.
            return null;
        } catch (Exception e) {
            throw rethrow(e);
        }
    }
}
