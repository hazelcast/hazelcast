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

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.Message;

import java.io.IOException;
import java.io.Serial;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * @param <M> type of message we receive from Pulsar
 * @param <T> type of message we send to later stages of the pipeline
 */
public class PulsarSourceConfig<M, T> extends PulsarConfig<M> implements DataSerializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private List<String> topics;
    private String subscriberName;
    private FunctionEx<Message<M>, T> projectionFn;
    private BatchReceivePolicy batchReceivePolicy;
    private ConsumerEx<ConsumerBuilder<M>> consumerCustomizer = ConsumerEx.noop();
    private ConsumerEx<ReaderBuilder<M>> readerCustomizer = ConsumerEx.noop();
    private int subscriptionInitialPosition = SubscriptionInitialPosition.Earliest.getValue();

    public PulsarSourceConfig() {
        super();
        batchReceivePolicy = BatchReceivePolicy.DEFAULT_POLICY;
        subscriberName = "jet-job-subscriber-" + UuidUtil.newUnsecureUuidString().toLowerCase(Locale.ROOT);
    }

    public PulsarSourceConfig(PulsarSourceConfig<M, T> other) {
        setTopics(List.copyOf(other.topics));
        subscriberName = other.subscriberName;
        projectionFn = other.projectionFn;
        batchReceivePolicy = other.batchReceivePolicy;
        consumerCustomizer = other.consumerCustomizer;
        readerCustomizer = other.readerCustomizer;
        setConnectionSupplier(other.getConnectionSupplier());
        setDataConnectionRef(other.getDataConnectionRef());
        setOptions(other.getOptions());
        setSchemaSupplier(other.getSchemaSupplier());
        setLocalParallelism(other.getLocalParallelism());
        setSubscriptionInitialPosition(other.getSubscriptionInitialPosition());
        setHttpServiceUrl(other.getHttpServiceUrl());
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public FunctionEx<Message<M>, T> getProjectionFn() {
        return projectionFn;
    }
    public BatchReceivePolicy getBatchReceivePolicy() {
        return batchReceivePolicy;
    }

    public ConsumerEx<ConsumerBuilder<M>> getConsumerCustomizer() {
        return consumerCustomizer;
    }
    public void setProjectionFn(FunctionEx<Message<M>, T> projectionFn) {
        this.projectionFn = projectionFn;
    }
    public void setBatchReceivePolicy(BatchReceivePolicy batchReceivePolicy) {
        this.batchReceivePolicy = batchReceivePolicy;
    }
    public void setConsumerCustomizer(ConsumerEx<ConsumerBuilder<M>> consumerCustomizer) {
        this.consumerCustomizer = consumerCustomizer;
    }

    public ConsumerEx<ReaderBuilder<M>> getReaderCustomizer() {
        return readerCustomizer;
    }

    public void setReaderCustomizer(ConsumerEx<ReaderBuilder<M>> readerCustomizer) {
        this.readerCustomizer = readerCustomizer;
    }

    public String getSubscriberName() {
        return subscriberName;
    }

    public void setSubscriberName(String subscriberId) {
        this.subscriberName = subscriberId;
    }

    public SubscriptionInitialPosition getSubscriptionInitialPosition() {
        return switch (subscriptionInitialPosition) {
            case 0 -> SubscriptionInitialPosition.Latest;
            case 1 -> SubscriptionInitialPosition.Earliest;
            default -> throw new IllegalArgumentException("Invalid subscription initial position: "
                                                              + subscriptionInitialPosition);
        };
    }

    public void setSubscriptionInitialPosition(SubscriptionInitialPosition subscriptionInitialPosition) {
        this.subscriptionInitialPosition = subscriptionInitialPosition.getValue();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeStringArray(topics.toArray(new String[0]));
        out.writeString(subscriberName);
        out.writeObject(projectionFn);
        out.writeObject(batchReceivePolicy);
        out.writeObject(consumerCustomizer);
        out.writeInt(subscriptionInitialPosition);
        out.writeObject(readerCustomizer);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        String[] topicsArray = in.readStringArray();
        topics = topicsArray == null ? List.of() : Arrays.asList(topicsArray);
        subscriberName = in.readString();
        projectionFn = in.readObject();
        batchReceivePolicy = in.readObject();
        consumerCustomizer = in.readObject();
        subscriptionInitialPosition = in.readInt();
        readerCustomizer = in.readObject();
    }

    public void validate() {
        var validation = commonValidation();

        validation
            .checkNotNull(getTopics(), "topics")
            .checkNotNullAndSerializable(projectionFn, "projectionFn")
            .checkNotNullAndSerializable(batchReceivePolicy, "batchReceivePolicy")
            .checkNotNullAndSerializable(consumerCustomizer, "consumerCustomizer")
            .checkNotNullAndSerializable(readerCustomizer, "readerCustomizer")
            .throwIfErrors();
    }
}
