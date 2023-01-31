/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.cdc;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.cdc.impl.ChangeRecordImpl;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.PipelineTestSupport;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.hazelcast.jet.cdc.Operation.DELETE;
import static com.hazelcast.jet.cdc.Operation.INSERT;
import static com.hazelcast.jet.cdc.Operation.SYNC;
import static com.hazelcast.jet.cdc.Operation.UPDATE;
import static java.util.Arrays.asList;

@Category(QuickTest.class)
public class CdcSinksTest extends PipelineTestSupport {

    private static final String MAP = "map";

    private static final String ID = "id";
    private static final String EMAIL = "email";

    private static final SupplierEx<ChangeRecord> SYNC1 = () -> changeRecord(0,  SYNC,
            "{\"" + ID + "\":1001}",
            null, "{\"" + ID + "\":1001,\"first_name\":\"Sally\",\"last_name\":\"Thomas\",\"" + EMAIL + "\":" +
                    "\"sally.thomas@acme.com\",\"__op\":\"r\",\"__ts_ms\":1588927306264,\"__deleted\":\"false\"}");
    private static final SupplierEx<ChangeRecord> INSERT2 = () -> changeRecord(1, INSERT,
            "{\"" + ID + "\":1002}",
            null, "{\"" + ID + "\":1002,\"first_name\":\"George\",\"last_name\":\"Bailey\",\"" + EMAIL + "\":" +
                    "\"gbailey@foobar.com\",\"__op\":\"c\",\"__ts_ms\":1588927306269,\"__deleted\":\"false\"}");
    private static final SupplierEx<ChangeRecord> UPDATE1 = () -> changeRecord(2,  UPDATE,
            "{\"" + ID + "\":1001}",
            null, "{\"" + ID + "\":1001,\"first_name\":\"Sally\",\"last_name\":\"Thomas\",\"" + EMAIL + "\":" +
                    "\"sthomas@acme.com\",\"__op\":\"u\",\"__ts_ms\":1588927306264,\"__deleted\":\"false\"}");
    private static final SupplierEx<ChangeRecord> DELETE2 = () -> changeRecord(3, DELETE,
            "{\"" + ID + "\":1002}",
            "{\"" + ID + "\":1002,\"first_name\":\"George\",\"last_name\":\"Bailey\",\"" + EMAIL + "\":" +
                    "\"gbailey@foobar.com\",\"__op\":\"d\",\"__ts_ms\":1588927306269,\"__deleted\":\"true\"}", null);

    private List<HazelcastInstance> remoteCluster;

    @After
    public void after() {
        if (remoteCluster != null) {
            for (HazelcastInstance instance : remoteCluster) {
                instance.getLifecycleService().terminate();
            }
            remoteCluster = null;
        }
    }

    @Test
    public void insertIntoLocalMap() {
        p.readFrom(items(SYNC1, INSERT2))
                .writeTo(localSync());
        execute().join();

        assertMap(hz(), "sally.thomas@acme.com", "gbailey@foobar.com");

        hz().getMap(MAP).destroy();
    }

    @Test
    public void insertIntoRemoteMap() {
        remoteCluster = createRemoteCluster(smallInstanceConfig().setClusterName(randomName()), 1);
        HazelcastInstance remoteInstance = remoteCluster.get(0);

        ClientConfig clientConfig = getClientConfigForRemoteCluster(remoteInstance);

        p.readFrom(items(SYNC1, INSERT2))
                .writeTo(remoteSync(clientConfig));
        execute().join();

        assertMap(remoteInstance, "sally.thomas@acme.com", "gbailey@foobar.com");

        remoteInstance.getMap(MAP).destroy();
    }

    @Test
    public void updateLocalMap() {
        p.readFrom(items(SYNC1, INSERT2, UPDATE1))
                .writeTo(localSync());
        execute().join();

        assertMap(hz(), "sthomas@acme.com", "gbailey@foobar.com");

        hz().getMap(MAP).destroy();
    }

    @Test
    public void updateRemoteMap() {
        remoteCluster = createRemoteCluster(smallInstanceConfig().setClusterName(randomName()), 1);
        HazelcastInstance remoteInstance = remoteCluster.get(0);

        ClientConfig clientConfig = getClientConfigForRemoteCluster(remoteInstance);

        p.readFrom(items(SYNC1, INSERT2, UPDATE1))
                .writeTo(remoteSync(clientConfig));
        execute().join();

        assertMap(remoteInstance, "sthomas@acme.com", "gbailey@foobar.com");

        remoteInstance.getMap(MAP).destroy();
    }

    @Test
    public void deleteFromLocalMap() {
        p.readFrom(items(SYNC1, INSERT2, DELETE2))
                .writeTo(localSync());
        execute().join();

        assertMap(hz(), "sally.thomas@acme.com", null);

        hz().getMap(MAP).destroy();
    }

    @Test
    public void deleteFromRemoteMap() {
        remoteCluster = createRemoteCluster(smallInstanceConfig().setClusterName(randomName()), 1);
        HazelcastInstance remoteInstance = remoteCluster.get(0);

        ClientConfig clientConfig = getClientConfigForRemoteCluster(remoteInstance);

        p.readFrom(items(SYNC1, INSERT2, DELETE2))
                .writeTo(remoteSync(clientConfig));
        execute().join();

        assertMap(remoteInstance, "sally.thomas@acme.com", null);

        remoteInstance.getMap(MAP).destroy();
    }

    @Test
    public void deleteFromLocalMap_ViaValueProjection() {
        p.readFrom(items(SYNC1, INSERT2))
                .writeTo(localSync());
        execute().join();

        p = Pipeline.create();
        p.readFrom(items(UPDATE1))
                .writeTo(CdcSinks.map(MAP,
                        r -> (Integer) r.key().toMap().get(ID),
                        r -> null
                ));
        execute().join();

        assertMap(hz(), null, "gbailey@foobar.com");

        hz().getMap(MAP).destroy();
    }

    @Test
    public void deleteFromRemoteMap_ViaValueProjection() {
        remoteCluster = createRemoteCluster(smallInstanceConfig().setClusterName(randomName()), 2);
        HazelcastInstance remoteInstance = remoteCluster.get(0);

        ClientConfig clientConfig = getClientConfigForRemoteCluster(remoteInstance);

        p.readFrom(items(SYNC1, INSERT2))
                .writeTo(remoteSync(clientConfig));
        execute().join();

        p = Pipeline.create();
        p.readFrom(items(UPDATE1))
                .writeTo(CdcSinks.remoteMap(MAP, clientConfig,
                        r -> (Integer) r.key().toMap().get(ID),
                        r -> null
                ));
        execute().join();

        assertMap(remoteInstance, null, "gbailey@foobar.com");

        remoteInstance.getMap(MAP).destroy();
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void reordering() {
        SupplierEx[] records = {
                SYNC1,
                UPDATE1,
                () -> changeRecord(10, UPDATE, UPDATE1.get().key().toJson(), null,
                        UPDATE1.get().value().toJson().replace("sthomas@acme.com", "sthomas2@acme.com")),
                () -> changeRecord(11, UPDATE, UPDATE1.get().key().toJson(), null,
                        UPDATE1.get().value().toJson().replace("sthomas@acme.com", "sthomas3@acme.com")),
                () -> changeRecord(12, UPDATE, UPDATE1.get().key().toJson(), null,
                        UPDATE1.get().value().toJson().replace("sthomas@acme.com", "sthomas4@acme.com")),
                () -> changeRecord(13, UPDATE, UPDATE1.get().key().toJson(), null,
                        UPDATE1.get().value().toJson().replace("sthomas@acme.com", "sthomas5@acme.com"))
        };
        p.readFrom(items(records))
                .rebalance()
                .map(r -> r)
                .writeTo(localSync());
        execute().join();

        assertMap(hz(), "sthomas5@acme.com", null);

        hz().getMap(MAP).destroy();
    }

    @Test
    public void reordering_syncUpdate() {
        p.readFrom(items(UPDATE1, SYNC1))
                .writeTo(localSync());
        execute().join();

        assertMap(hz(), "sthomas@acme.com", null);

        hz().getMap(MAP).destroy();
    }

    @Test
    public void reordering_insertDelete() {
        p.readFrom(items(DELETE2, INSERT2))
                .writeTo(localSync());
        execute().join();

        assertMap(hz(), null, null);

        hz().getMap(MAP).destroy();
    }

    @Test
    public void reordering_differentIds() {
        p.readFrom(items(DELETE2, UPDATE1, INSERT2, SYNC1))
                .writeTo(localSync());
        execute().join();

        assertMap(hz(), "sthomas@acme.com", null);

        hz().getMap(MAP).destroy();
    }

    @Test
    public void deleteWithoutInsertNorUpdate() {
        p.readFrom(items(SYNC1, DELETE2))
                .writeTo(localSync());
        execute().join();

        assertMap(hz(), "sally.thomas@acme.com", null);

        hz().getMap(MAP).destroy();
    }

    @Test
    public void sourceSwitch() {
        String updatedKey = UPDATE1.get().key().toJson();
        String updatedJson = UPDATE1.get().value().toJson().replace("sthomas@acme.com", "sthomas2@acme.com");
        SupplierEx<ChangeRecord> changedRecord = () -> changeRecord(4, UPDATE, updatedKey, null, updatedJson);

        p.readFrom(items(UPDATE1, INSERT2, changedRecord))
                .writeTo(localSync())
                .setLocalParallelism(1);

        execute().join();

        assertMap(hz(), "sthomas2@acme.com", "gbailey@foobar.com");

        hz().getMap(MAP).destroy();
    }

    private void assertMap(HazelcastInstance instance, String email1, String email2) {
        Map<Integer, String> expectedMap = new HashMap<>();
        if (email1 != null) {
            expectedMap.put(1001, email1);
        }
        if (email2 != null) {
            expectedMap.put(1002, email2);
        }

        assertEqualsEventually(getActualMap(instance), expectedMap);
    }

    private Callable<Map<?, ?>> getActualMap(HazelcastInstance instace) {
        return () -> instace.getMap(MAP).entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    private Sink<ChangeRecord> localSync() {
        return CdcSinks.map(MAP,
                r -> (Integer) r.key().toMap().get(ID),
                r -> (String) r.value().toMap().get(EMAIL)
        );
    }

    private Sink<ChangeRecord> remoteSync(ClientConfig clientConfig) {
        return CdcSinks.remoteMap(MAP, clientConfig,
                r -> (Integer) r.key().toMap().get(ID),
                r -> (String) r.value().toMap().get(EMAIL)
        );
    }

    @SafeVarargs
    private static <T> BatchSource<T> items(@Nonnull SupplierEx<T>... items) {
        final SupplierEx<List<Supplier<T>>> listSupplier = () -> asList(items);
        return SourceBuilder.batch("items", ctx -> null)
                .<T>fillBufferFn((ignored, buf) -> {
                    listSupplier.get().forEach(item -> buf.add(item.get()));
                    buf.close();
                })
                .distributed(1)
                .build();
    }

    private static ChangeRecord changeRecord(
            int sequenceValue, Operation operation,
            String keyJson, String oldValueJson, String newValueJson
    ) {
        return new ChangeRecordImpl(0, 0, sequenceValue,  operation,
                keyJson,
                oldValueJson == null ? null : () -> oldValueJson, newValueJson == null ? null : () -> newValueJson,
                "t", "s", "d");
    }

}
