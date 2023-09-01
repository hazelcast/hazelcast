/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.avro.impl;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.map.IMap;
import org.apache.avro.Schema;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.jet.impl.JobRepository.INTERNAL_JET_OBJECTS_PREFIX;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.apache.avro.SchemaNormalization.parsingFingerprint64;

/**
 * Schemas are stored in an IMap, where the key is the 64-bit Rabin fingerprint of the
 * canonical JSON representation of the schema. As explained in the
 * <a href="https://avro.apache.org/docs/1.11.1/specification/#schema-fingerprints">Avro
 * specification</a>, such fingerprints are safe to be used as a key in schema caches of up
 * to a million entries (for such a cache, the chance of a collision is 3x10<sup>-8</sup>).
 * <p>
 * Schemas that are not used for one week are dropped and the expiration time of the most
 * recently used schemas are updated once an hour in batches.
 */
public class AvroSchemaStorage {
    private static final String AVRO_SCHEMAS_MAP_NAME = INTERNAL_JET_OBJECTS_PREFIX + "avro.schemas";
    private static final int AVRO_SCHEMAS_MAX_IDLE_SECONDS = (int) DAYS.toSeconds(7);

    private final Map<Long, Schema> localSchemas = new ConcurrentHashMap<>();
    private final Map<Schema, Long> localSchemaIds = new ConcurrentHashMap<>();

    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private final AtomicReference<Map<Long, Schema>> mostRecentlyUsed = new AtomicReference<>(new HashMap<>());

    private IMap<Long, Schema> schemas;

    public AvroSchemaStorage(HazelcastInstance instance) {
        instance.getLifecycleService().addLifecycleListener(event -> {
            if (event.getState() == LifecycleEvent.LifecycleState.STARTED) {
                instance.getConfig().addMapConfig(new MapConfig(AVRO_SCHEMAS_MAP_NAME)
                        .setMaxIdleSeconds(AVRO_SCHEMAS_MAX_IDLE_SECONDS));
                schemas = instance.getMap(AVRO_SCHEMAS_MAP_NAME);
                schemas.addEntryListener(new EntryAdapter<Integer, String>() {
                    @Override
                    public void onEntryEvent(EntryEvent<Integer, String> e) {
                        long schemaId = e.getKey();
                        Schema schema = new Schema.Parser().parse(e.getValue());
                        switch (e.getEventType()) {
                            case ADDED:
                                localSchemas.put(schemaId, schema);
                                localSchemaIds.put(schema, schemaId);
                                break;
                            case EVICTED:
                                localSchemas.remove(schemaId);
                                localSchemaIds.remove(schema);
                                break;
                            default:
                                throw new UnsupportedOperationException(
                                        "Avro Schema Storage does not support " + e.getEventType());
                        }
                    }
                }, true);

                // Update the expiration time of most recently used schemas periodically.
                executorService.scheduleAtFixedRate(() ->
                        schemas.putAll(mostRecentlyUsed.getAndSet(new HashMap<>())), 1, 1, HOURS);
            }
        });
    }

    public long put(Schema schema) {
        Long schemaId = localSchemaIds.get(schema);
        if (schemaId == null) {
            schemaId = parsingFingerprint64(schema);
            schemas.put(schemaId, schema);
            localSchemas.put(schemaId, schema);
            localSchemaIds.put(schema, schemaId);
        } else {
            mostRecentlyUsed.get().put(schemaId, schema);
        }
        return schemaId;
    }

    public Schema get(long schemaId) {
        Schema schema = localSchemas.get(schemaId);
        if (schema == null) {
            schema = schemas.get(schemaId);
            localSchemas.put(schemaId, schema);
            localSchemaIds.put(schema, schemaId);
        } else {
            mostRecentlyUsed.get().put(schemaId, schema);
        }
        return schema;
    }
}
