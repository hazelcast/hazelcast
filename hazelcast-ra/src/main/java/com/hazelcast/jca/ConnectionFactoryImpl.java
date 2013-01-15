/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jca;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.resource.ResourceException;
import javax.resource.cci.*;
import javax.resource.spi.ConnectionManager;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionFactoryImpl extends JcaBase implements ConnectionFactory {
    final ManagedConnectionFactoryImpl mcf;
    final ConnectionManager cm;
    private Reference ref;
    private final static AtomicInteger idGen = new AtomicInteger();
    private transient final int id;

    public ConnectionFactoryImpl(ManagedConnectionFactoryImpl mcf, ConnectionManager cm) {
        super();
        this.mcf = mcf;
        this.cm = cm;
        id = idGen.incrementAndGet();
    }

    public Connection getConnection() throws ResourceException {
        log(this, "getConnection");
        return (Connection) cm.allocateConnection(mcf, null);
    }

    public Connection getConnection(ConnectionSpec connSpec) throws ResourceException {
        log(this, "getConnection spec: " + connSpec);
        return (Connection) cm.allocateConnection(mcf, null);
    }

    public ResourceAdapterMetaData getMetaData() throws ResourceException {
        return null;
    }

    public RecordFactory getRecordFactory() throws ResourceException {
        return null;
    }

    public void setReference(Reference ref) {
        this.ref = ref;
    }

    public Reference getReference() throws NamingException {
        return ref;
    }

    @Override
    public String toString() {
        return "hazelcast.ConnectionFactoryImpl [" + id + "]";
    }
}
