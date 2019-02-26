/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.MetadataInitializer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Metadata;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

import static com.hazelcast.util.EmptyStatement.ignore;
import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * Creates json related metadata for map records.
 * Metadata is created and set for put and update operations. There is
 * no need for removing metadata on remove events because metadata is
 * stored inside record. It is removed with the record.
 */
public class JsonMetadataRecordStoreMutationObserver implements RecordStoreMutationObserver<Record> {

    private InternalSerializationService serializationService;
    private MetadataInitializer metadataInitializer;

    public JsonMetadataRecordStoreMutationObserver(InternalSerializationService serializationService,
                                                   MetadataInitializer metadataInitializer) {
        this.serializationService = serializationService;
        this.metadataInitializer = metadataInitializer;
    }

    @Override
    public void onClear() {
        // no-op
    }

    @Override
    public void onPutRecord(Data key, Record record) {
        onPutInternal(record);
    }

    @Override
    public void onReplicationPutRecord(Data key, Record record) {
        onPutInternal(record);
    }

    @Override
    public void onUpdateRecord(Data key, Record record, Object newValue) {
        updateValueMetadataIfNeccessary(record, newValue);
    }

    @Override
    public void onRemoveRecord(Data key, Record record) {
        // no-op
    }

    @Override
    public void onEvictRecord(Data key, Record record) {
        // no-op
    }

    @Override
    public void onLoadRecord(Data key, Record record) {
        onPutInternal(record);
    }

    @Override
    public void onDestroy(boolean internal) {
        // no-op
    }

    @Override
    public void onReset() {
        // no-op
    }

    protected Metadata getMetadata(Record record) {
        return record.getMetadata();
    }

    protected void setMetadata(Record record, Metadata metadata) {
        record.setMetadata(metadata);
    }

    protected void removeMetadata(Record record) {
        record.setMetadata(null);
    }

    private void onPutInternal(Record record) {
        Metadata metadata = initializeMetadata(record.getKey(), record.getValue());
        if (metadata != null) {
            setMetadata(record, metadata);
        }
    }

    @SuppressFBWarnings("NP_LOAD_OF_KNOWN_NULL_VALUE")
    private void updateValueMetadataIfNeccessary(Record record, Object updateValue) {
        Object valueMetadata = null;
        try {
            if (record.getValue() instanceof Data) {
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
        if (valueMetadata != null) {
            // There is some valueMetadata. We either set existing record.valueMetadata or create a new one.
            Metadata existing = getMetadata(record);
            if (existing == null) {
                existing = new Metadata();
                setMetadata(record, existing);
            }
            existing.setValueMetadata(valueMetadata);
        } else {
            // Value metadata is empty. We either remove metadata altogether (if keyMetadata is null too)
            // or set valueMetadata to null.
            Metadata existing = getMetadata(record);
            if (existing != null) {
                if (existing.getKeyMetadata() == null) {
                    removeMetadata(record);
                } else {
                    existing.setValueMetadata(valueMetadata);
                }
            }
        }
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
                Metadata metadata = new Metadata();
                metadata.setKeyMetadata(keyMetadata);
                metadata.setValueMetadata(valueMetadata);
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
