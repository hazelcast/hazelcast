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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.MetadataInitializer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.impl.JsonMetadata;
import com.hazelcast.query.impl.Metadata;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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

    public JsonMetadataMutationObserver(SerializationService serializationService,
                                        MetadataInitializer metadataInitializer,
                                        JsonMetadataStore metadataStore) {
        this.serializationService = serializationService;
        this.metadataInitializer = metadataInitializer;
        this.metadataStore = metadataStore;
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
        updateValueMetadataIfNecessary(key, oldValue, newValue);
    }

    @Override
    public void onLoadRecord(@Nonnull Data key, @Nonnull Record record, boolean backup) {
        onPutInternal(key, record);
    }

    @Override
    public void onRemoveRecord(Data key, Record record) {
        metadataStore.remove(key);
    }

    @Override
    public void onEvictRecord(Data key, Record record) {
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

    @SuppressFBWarnings("NP_LOAD_OF_KNOWN_NULL_VALUE")
    private void updateValueMetadataIfNecessary(Data dataKey,
                                                Object oldValue, Object updateValue) {
        Object valueMetadata = null;
        try {
            if (oldValue instanceof Data) {
                valueMetadata = metadataInitializer.createFromData(serializationService.toData(updateValue));
            } else {
                valueMetadata = metadataInitializer.createFromObject(serializationService.toObject(updateValue));
            }
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
            if (value instanceof Data) {
                valueMetadata = metadataInitializer.createFromData((Data) value);
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
