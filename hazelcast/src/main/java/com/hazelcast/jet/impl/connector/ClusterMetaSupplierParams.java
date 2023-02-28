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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.internal.journal.EventJournalReader;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.pipeline.DataLinkRef;
import com.hazelcast.jet.pipeline.JournalInitialPosition;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.security.Permission;

/**
 * Parameter object class
 *
 * @param <E> is the type of EventJournalMapEvent
 * @param <T> is the return type of the stream
 */
class ClusterMetaSupplierParams<E, T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Optional XML to create a HazelcastInstance which is client that is used to connect to cluster members
     */
    private String clientXml;

    /**
     * Optional DataLinkRef that refers a HzClientDataLinkFactory by name.
     * The HzClientDataLinkFactory owns a HazelcastInstance which is client that is used to connect to cluster members
     */
    private DataLinkRef dataLinkRef;

    /**
     * The function that creates EventJournalReader
     */
    private FunctionEx<? super HazelcastInstance, ? extends EventJournalReader<E>> eventJournalReaderSupplier;

    /**
     * The predicate and projection for
     * <pre>{@code
     * eventJournalReader.readFromEventJournal(offset, 1, MAX_FETCH_SIZE, partition, predicate, projection)
     * }</pre>
     */
    private PredicateEx<? super E> predicate;
    private FunctionEx<? super E, ? extends T> projection;
    private JournalInitialPosition initialPos;
    /**
     * The parameter for StreamEventJournalP
     * <pre> {@code
     * eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
     * }
     * </pre>
     */
    private EventTimePolicy<? super T> eventTimePolicy;
    private SupplierEx<Permission> permissionFn;

    /**
     * Create parameter object for connecting to a remote cluster using given XML
     * @param <E> is the type of EventJournalMapEvent
     * @param <T> is the return type of the stream
     */
    public static <E, T> ClusterMetaSupplierParams<E, T> fromXML(@Nonnull String clientXml) {
        ClusterMetaSupplierParams<E, T> params = new ClusterMetaSupplierParams<>();
        params.setClientXml(clientXml);
        return params;
    }

    /**
     * Create parameter object for connecting to a remote cluster using given DataLinkRef
     * @param <E> is the type of EventJournalMapEvent
     * @param <T> is the return type of the stream
     */
    public static <E, T> ClusterMetaSupplierParams<E, T> fromDataLinkRef(@Nonnull DataLinkRef dataLinkRef) {
        ClusterMetaSupplierParams<E, T> params = new ClusterMetaSupplierParams<>();
        params.setDataLinkRef(dataLinkRef);
        return params;
    }

    /**
     * Create parameter object for local cluster
     * @param <E> is the type of EventJournalMapEvent
     * @param <T> is the return type of the stream
     */
    public static <E, T> ClusterMetaSupplierParams<E, T> empty() {
        return new ClusterMetaSupplierParams<>();
    }

    public String getClientXml() {
        return clientXml;
    }

    private void setClientXml(@Nonnull String clientXml) {
        this.clientXml = clientXml;
    }

    public DataLinkRef getDataLinkRef() {
        return dataLinkRef;
    }

    private void setDataLinkRef(@Nonnull DataLinkRef dataLinkRef) {
        this.dataLinkRef = dataLinkRef;
    }

    @Nonnull
    public FunctionEx<? super HazelcastInstance, ? extends EventJournalReader<E>> getEventJournalReaderSupplier() {
        return eventJournalReaderSupplier;
    }

    public void setEventJournalReaderSupplier(
            @Nonnull FunctionEx<? super HazelcastInstance, ? extends EventJournalReader<E>> eventJournalReaderSupplier) {
        this.eventJournalReaderSupplier = eventJournalReaderSupplier;
    }

    @Nonnull
    public PredicateEx<? super E> getPredicate() {
        return predicate;
    }

    public void setPredicate(@Nonnull PredicateEx<? super E> predicate) {
        this.predicate = predicate;
    }

    @Nonnull
    public FunctionEx<? super E, ? extends T> getProjection() {
        return projection;
    }

    public void setProjection(@Nonnull FunctionEx<? super E, ? extends T> projection) {
        this.projection = projection;
    }

    @Nonnull
    public JournalInitialPosition getInitialPos() {
        return initialPos;
    }

    public void setInitialPos(@Nonnull JournalInitialPosition initialPos) {
        this.initialPos = initialPos;
    }

    @Nonnull
    public EventTimePolicy<? super T> getEventTimePolicy() {
        return eventTimePolicy;
    }

    public void setEventTimePolicy(@Nonnull EventTimePolicy<? super T> eventTimePolicy) {
        this.eventTimePolicy = eventTimePolicy;
    }

    @Nonnull
    public SupplierEx<Permission> getPermissionFn() {
        return permissionFn;
    }

    public void setPermissionFn(@Nonnull SupplierEx<Permission> permissionFn) {
        this.permissionFn = permissionFn;
    }


}
