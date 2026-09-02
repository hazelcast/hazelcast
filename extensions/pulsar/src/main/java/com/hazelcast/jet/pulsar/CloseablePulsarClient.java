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
package com.hazelcast.jet.pulsar;

import com.hazelcast.function.ThrowingRunnable;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableViewBuilder;
import org.apache.pulsar.client.api.transaction.TransactionBuilder;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

final class CloseablePulsarClient implements PulsarClient {
    private final PulsarClient delegate;
    private final ThrowingRunnable closeAction;
    private final Supplier<Boolean> isDestroyed;
    private final ThrowingRunnable shutdownAction;

    CloseablePulsarClient(PulsarClient delegate,
                          ThrowingRunnable closeAction,
                          ThrowingRunnable shutdownAction,
                          Supplier<Boolean> isDestroyed) {
        this.delegate = delegate;
        this.closeAction = closeAction;
        this.shutdownAction = shutdownAction;
        this.isDestroyed = isDestroyed;
    }

    public PulsarClient unwrap() {
        return delegate;
    }

    @Override
    public void close() {
        closeAction.run();
    }

    @Override
    public boolean isClosed() {
        return isDestroyed.get();
    }

    @Override
    public ProducerBuilder<byte[]> newProducer() {
        return delegate.newProducer();
    }

    @Override
    public <T> ProducerBuilder<T> newProducer(Schema<T> schema) {
        return delegate.newProducer(schema);
    }

    @Override
    public ConsumerBuilder<byte[]> newConsumer() {
        return delegate.newConsumer();
    }

    @Override
    public <T> ConsumerBuilder<T> newConsumer(Schema<T> schema) {
        return delegate.newConsumer(schema);
    }

    @Override
    public ReaderBuilder<byte[]> newReader() {
        return delegate.newReader();
    }

    @Override
    public <T> ReaderBuilder<T> newReader(Schema<T> schema) {
        return delegate.newReader(schema);
    }

    @Override
    @SuppressWarnings("deprecation")
    public <T> TableViewBuilder<T> newTableViewBuilder(Schema<T> schema) {
        return delegate.newTableViewBuilder(schema);
    }

    @Override
    public TableViewBuilder<byte[]> newTableView() {
        return delegate.newTableView();
    }

    @Override
    public <T> TableViewBuilder<T> newTableView(Schema<T> schema) {
        return delegate.newTableView(schema);
    }

    @Override
    public void updateServiceUrl(String serviceUrl) throws PulsarClientException {
        delegate.updateServiceUrl(serviceUrl);
    }

    @Override
    public CompletableFuture<List<String>> getPartitionsForTopic(String topic, boolean metadataAutoCreationEnabled) {
        return delegate.getPartitionsForTopic(topic, metadataAutoCreationEnabled);
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return CompletableFuture.runAsync(closeAction);
    }

    @Override
    public void shutdown() {
        shutdownAction.run();
    }

    @Override
    public TransactionBuilder newTransaction() {
        return delegate.newTransaction();
    }
}
