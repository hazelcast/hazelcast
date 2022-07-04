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

package com.hazelcast.map.impl.record;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryMetadata;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryMetadataImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.map.impl.record.Record.NOT_CACHED;
import static com.hazelcast.map.impl.record.RecordReaderWriter.getById;

/**
 * Contains various factory &amp; helper methods for a {@link
 * com.hazelcast.map.impl.record.Record} object.
 */
public final class Records {

    private Records() {
    }

    public static void writeRecord(ObjectDataOutput out, Record record,
                                   Data dataValue) throws IOException {
        RecordReaderWriter readerWriter = record.getMatchingRecordReaderWriter();
        out.writeByte(readerWriter.getId());
        readerWriter.writeRecord(out, record, dataValue);
    }

    public static Record readRecord(ObjectDataInput in) throws IOException {
        byte matchingDataRecordId = in.readByte();
        return getById(matchingDataRecordId).readRecord(in);
    }

    public static void writeExpiry(ObjectDataOutput out,
                                   ExpiryMetadata expiryMetadata) throws IOException {
        boolean hasExpiry = expiryMetadata.hasExpiry();
        out.writeBoolean(hasExpiry);
        if (hasExpiry) {
            expiryMetadata.write(out);
        }
    }

    public static ExpiryMetadata readExpiry(ObjectDataInput in) throws IOException {
        ExpiryMetadata expiryMetadata = ExpiryMetadata.NULL;
        boolean hasExpiry = in.readBoolean();
        if (hasExpiry) {
            expiryMetadata = new ExpiryMetadataImpl();
            expiryMetadata.read(in);
        }
        return expiryMetadata;
    }

    /**
     * Except transient field {@link com.hazelcast.query.impl.Metadata},
     * all record-metadata is copied from one record to another.
     */
    public static void copyMetadataFrom(Record fromRecord, Record toRecord) {
        if (fromRecord == null) {
            return;
        }
        toRecord.setHits(fromRecord.getHits());
        toRecord.setVersion(fromRecord.getVersion());
        toRecord.setCreationTime(fromRecord.getCreationTime());
        toRecord.setLastAccessTime(fromRecord.getLastAccessTime());
        toRecord.setLastStoredTime(fromRecord.getLastStoredTime());
        toRecord.setLastUpdateTime(fromRecord.getLastUpdateTime());
    }

    /**
     * Get current cached value from the record.
     * This method protects you against accidental exposure
     * of cached value mutex into rest of the code.
     * <p>
     * Use it instead of raw {@link Record#getCachedValueUnsafe()} See
     * {@link #getValueOrCachedValue(Record, SerializationService)}
     * for details.
     *
     * @param record
     * @return
     */
    public static Object getCachedValue(Record record) {
        for (; ; ) {
            Object cachedValue = record.getCachedValueUnsafe();
            if (!(cachedValue instanceof Thread)) {
                return cachedValue;
            }

            cachedValue = ThreadWrapper.unwrapOrNull(cachedValue);
            if (cachedValue != null) {
                return cachedValue;
            }
        }
    }

    /**
     * Return cached value where appropriate,
     * otherwise return the actual value.
     * Value caching makes sense when:
     * <ul>
     * <li>OBJECT InMemoryFormat is not used</li>
     * <li>Portable serialization is not used</li>
     * <li>HazelcastJsonValue objects are not used</li>
     * </ul>
     * <p>
     * If Record does not contain cached value and is found
     * appropriate (see above) then new cache value is created
     * by de-serializing the {@link Record#getValue()}
     * <p>
     * The newly de-deserialized value may not be stored into the Record
     * cache when the record has been modified while the method was running.
     * <p>
     * WARNING: This method may temporarily set an arbitrary object into the
     * Record cache - this object acts as mutex. The mutex should never be
     * returned to the outside world. Use {@link #getCachedValue(Record)}
     * instead of raw {@link Record#getCachedValueUnsafe()} to
     * protect from accidental mutex exposure to the user-code.
     *
     * @param record
     * @param serializationService
     * @return
     */
    public static Object getValueOrCachedValue(Record record, SerializationService serializationService) {
        Object cachedValue = record.getCachedValueUnsafe();
        if (cachedValue == NOT_CACHED) {
            //record does not support caching at all
            return record.getValue();
        }
        for (; ; ) {
            if (cachedValue == null) {
                Object valueBeforeCas = record.getValue();
                if (!shouldCache(valueBeforeCas)) {
                    //it's either a null or value which we do not want to cache. let's just return it.
                    return valueBeforeCas;
                }
                Object fromCache = tryStoreIntoCache(record, valueBeforeCas, serializationService);
                if (fromCache != null) {
                    return fromCache;
                }
            } else if (cachedValue instanceof Thread) {
                //the cachedValue is either locked by another thread or it contains a wrapped thread
                cachedValue = ThreadWrapper.unwrapOrNull(cachedValue);
                if (cachedValue != null) {
                    //exceptional case: the cachedValue is not locked, it just contains an instance of Thread.
                    //this can happen when user put an instance of Thread into a map
                    //(=it should never happen, but never say never...)
                    return cachedValue;
                }
                //it looks like some other thread actually locked the cachedValue. let's give it another try (iteration)
            } else {
                //it's not the 'in-progress' marker/lock && it's not a null -> it has to be the actual cachedValue
                return cachedValue;
            }
            Thread.yield();
            cachedValue = record.getCachedValueUnsafe();
        }
    }

    private static Object tryStoreIntoCache(Record record, Object valueBeforeCas, SerializationService serializationService) {
        Thread currentThread = Thread.currentThread();
        if (!record.casCachedValue(null, currentThread)) {
            return null;
        }

        //we managed to lock the record for ourselves
        Object valueAfterCas = record.getValue();
        Object object = null;
        try {
            object = serializationService.toObject(valueBeforeCas);
        } catch (Exception e) {
            record.casCachedValue(currentThread, null);
            throw e;
        }
        if (valueAfterCas == valueBeforeCas) {
            //this check is needed to make sure a partition thread had not changed the value
            //right before we won the CAS
            Object wrappedObject = ThreadWrapper.wrapIfNeeded(object);
            record.casCachedValue(currentThread, wrappedObject);
            //we can return the object no matter of the CAS outcome. if we lose the CAS it means
            //the value had been mutated concurrently and partition thread removed our lock.
        } else {
            //the value has changed -> we can return the object to the caller as it was valid at some point in time
            //we are just not storing it into the cache as apparently it's not valid anymore.

            //we have to CAS the lock out as it could had been already removed by the partition thread
            record.casCachedValue(currentThread, null);

        }
        return object;
    }

    static boolean shouldCache(Object value) {
        // For portables, we cannot extract information from the deserialized form.
        // For HazelcastJsonValue objects, if we pass the instanceof Data check, that
        // means the metadata is created from the Data representation of the object.
        // If we allow using the deserialized values, the metadata might not be safe to use.
        if (value instanceof Data) {
            Data data = (Data) value;
            return !(data.isPortable() || data.isJson() || data.isCompact());
        }
        return false;
    }


    /**
     * currentThread inside cachedValue acts as "deserialization in-progress" marker
     * if the actual deserialized value is instance of Thread then we need to wrap it
     * otherwise it might be mistaken for the "deserialization in-progress" marker.
     */
    private static final class ThreadWrapper extends Thread {
        private final Thread wrappedValue;

        private ThreadWrapper(Thread wrappedValue) {
            this.wrappedValue = wrappedValue;
        }

        static Object unwrapOrNull(Object o) {
            if (o instanceof ThreadWrapper) {
                return ((ThreadWrapper) o).wrappedValue;
            }
            return null;
        }

        static Object wrapIfNeeded(Object object) {
            if (object instanceof Thread) {
                //exceptional case: deserialized value is an instance of Thread
                //we need to wrap it as we use currentThread to mark the cacheValue is 'deserilization in-progress'
                //this is the only case where we allocate a new object.
                return new ThreadWrapper((Thread) object);
            }
            return object;
        }
    }

}
