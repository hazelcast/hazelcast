/*
 * Copyright 2026 Hazelcast Inc.
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
package com.hazelcast.jet.pulsar.impl;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.util.Validation;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

import static com.hazelcast.jet.pulsar.impl.Utils.exactlyOnlyOneIsNotNull;

/**
 * Object encapsulating common Pulsar connector properties.
 * @param <M> the type of messages received or read from Pulsar (in {@link Message#getValue()}).
 */
public class PulsarConfig<M> implements DataSerializable, Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private SupplierEx<PulsarClient> connectionSupplier;
    private String httpServiceUrl;
    private DataConnectionRef dataConnectionRef;
    private SupplierEx<Schema<M>> schemaSupplier;

    private Map<String, Object> options;
    private int localParallelism = -1;

    // needed for serialization
    @SuppressWarnings("unused")
    public PulsarConfig() {
    }

    public PulsarConfig(SupplierEx<PulsarClient> connectionSupplier,
                 DataConnectionRef dataConnectionRef,
                 SupplierEx<Schema<M>> schemaSupplier,
                 Map<String, Object> options) {
        this.connectionSupplier = connectionSupplier;
        this.dataConnectionRef = dataConnectionRef;
        this.schemaSupplier = schemaSupplier;
        this.options = options;
    }

    @Nullable
    public SupplierEx<PulsarClient> getConnectionSupplier() {
        return connectionSupplier;
    }

    @Nonnull
    public SupplierEx<PulsarClient> getTargetConnectionSupplier(@Nonnull ProcessorSupplier.Context ctx) {
        return () -> Utils.getClient(ctx, connectionSupplier, dataConnectionRef);
    }

    @Nullable
    public DataConnectionRef getDataConnectionRef() {
        return dataConnectionRef;
    }

    public SupplierEx<Schema<M>> getSchemaSupplier() {
        return schemaSupplier;
    }

    public Map<String, Object> getOptions() {
        return options;
    }

    public void setConnectionSupplier(SupplierEx<PulsarClient> connectionSupplier) {
        this.connectionSupplier = connectionSupplier;
    }

    public void setDataConnectionRef(DataConnectionRef dataConnectionRef) {
        this.dataConnectionRef = dataConnectionRef;
    }

    public void setSchemaSupplier(SupplierEx<Schema<M>> schemaSupplier) {
        this.schemaSupplier = schemaSupplier;
    }

    public void setOptions(Map<String, Object> options) {
        this.options = options;
    }

    public int getLocalParallelism() {
        return localParallelism;
    }

    public void setLocalParallelism(int localParallelism) {
        this.localParallelism = localParallelism;
    }

    public String getHttpServiceUrl() {
        return httpServiceUrl;
    }

    public void setHttpServiceUrl(String httpServiceUrl) {
        this.httpServiceUrl = httpServiceUrl;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(connectionSupplier);
        out.writeString(dataConnectionRef == null ? null : dataConnectionRef.getName());
        out.writeObject(schemaSupplier);
        out.writeObject(options);
        out.writeInt(localParallelism);
        out.writeString(httpServiceUrl);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        connectionSupplier = in.readObject();
        String ref = in.readString();
        dataConnectionRef = ref == null ? null : DataConnectionRef.dataConnectionRef(ref);
        schemaSupplier = in.readObject();
        options = in.readObject();
        localParallelism = in.readInt();
        httpServiceUrl = in.readString();
    }

    protected Validation commonValidation() {
        return Validation.validate()
                  .check(exactlyOnlyOneIsNotNull(getConnectionSupplier(), getDataConnectionRef()),
                   "Either connectionSupplier or dataConnectionRef must be provided")
                  .checkSerializableIfNotNull(getConnectionSupplier(), "connectionSupplier")

                  .checkNotNull(getOptions(), "options")
                  .checkNotNullAndSerializable(getSchemaSupplier(), "schemaSupplier");
    }
}
