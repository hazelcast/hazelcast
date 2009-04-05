/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.jca;

import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;
import javax.security.auth.Subject;

public class ManagedConnectionFactoryImpl extends JcaBase implements ManagedConnectionFactory {
    private PrintWriter printWriter = null;
    private final static AtomicInteger idGen = new AtomicInteger();
    private transient final int id;

    public ManagedConnectionFactoryImpl() {
        id = idGen.incrementAndGet();
    }

    public Object createConnectionFactory() throws ResourceException {
        return createConnectionFactory(null);
    }

    public Object createConnectionFactory(ConnectionManager cm) throws ResourceException {
        log(this, "createConnectionFactory cm: " + cm);
        return new ConnectionFactoryImpl(this, cm);
    }

    public ManagedConnection createManagedConnection(Subject arg0, ConnectionRequestInfo arg1)
            throws ResourceException {
        log(this, "createManagedConnection");
        return new ManagedConnectionImpl();
    }

    public void setLogWriter(PrintWriter printWriter) throws ResourceException {
        this.printWriter = printWriter;
    }

    public PrintWriter getLogWriter() throws ResourceException {
        return printWriter;
    }

    public ManagedConnection matchManagedConnections(Set set, Subject subject,
                                                     ConnectionRequestInfo connectionRequestInfo) throws ResourceException {
        log(this, "matchManagedConnections");
        if (set == null || set.size() == 0)
            return null;
        Iterator it = set.iterator();
        if (it.hasNext()) {
            return (ManagedConnection) it.next();
        } else {
            return null;
        }
    }

    @Override
    public String toString() {
        return "hazelcast.ManagedConnectionFactoryImpl [" + id + "]";
    }
}
