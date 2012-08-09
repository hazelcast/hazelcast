/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import static java.util.Collections.emptySet;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterAssociation;
import javax.security.auth.Subject;

public class ManagedConnectionFactoryImpl extends JcaBase implements ManagedConnectionFactory, ResourceAdapterAssociation {
	private static final long serialVersionUID = -4889598421534961926L;
	private static final AtomicInteger idGen = new AtomicInteger();
	
	private transient final int id;

	private Set<HzConnectionEvent> hzConnectionTracingEvents;
	private ResourceAdapterImpl resourceAdapter;
	private boolean connectionTracingDetail;

	public ManagedConnectionFactoryImpl() {
		id = idGen.incrementAndGet();
	}

	public Object createConnectionFactory() throws ResourceException {
		return createConnectionFactory(null);
	}

	public Object createConnectionFactory(ConnectionManager cm)
			throws ResourceException {
		log(Level.FINEST, "createConnectionFactory cm: " + cm);
		logHzConnectionEvent(this, HzConnectionEvent.FACTORY_INIT);
		return new ConnectionFactoryImpl(this, cm);
	}

	public ManagedConnection createManagedConnection(Subject subject,
			ConnectionRequestInfo cxRequestInfo) throws ResourceException {
		log(Level.FINEST, "createManagedConnection");

		return new ManagedConnectionImpl(cxRequestInfo, this);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ManagedConnectionFactoryImpl other = (ManagedConnectionFactoryImpl) obj;
		if (id != other.id)
			return false;
		return true;
	}

	public String getConnectionTracingEvents() {
		return this.hzConnectionTracingEvents.toString();
	}

	public ResourceAdapterImpl getResourceAdapter() {
		return resourceAdapter;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		return result;
	}

	void logHzConnectionEvent(Object eventSource, HzConnectionEvent event) {
		if (hzConnectionTracingEvents.contains(event)) {
			StringBuilder sb = new StringBuilder("HZ Connection Event <<");
			sb.append(event).append(">> for ").append(eventSource);
			sb.append(" in thread [").append(Thread.currentThread().getName());
			sb.append("]");

			if (isConnectionTracingDetail()) {
				log(Level.INFO, sb.toString());
			} else {
				log(Level.INFO, sb.toString(), //
						new Exception("Hz Connection Event Call Stack"));
			}
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public ManagedConnection matchManagedConnections(Set connectionSet,
			Subject subject, ConnectionRequestInfo cxRequestInfo)
			throws ResourceException {
		log(Level.FINEST, "matchManagedConnections");

		if ((null != connectionSet) && !connectionSet.isEmpty()) {
			for (ManagedConnection conn : (Set<ManagedConnection>) connectionSet) {
				if (conn instanceof ManagedConnectionImpl) {
					ConnectionRequestInfo otherCxRequestInfo = ((ManagedConnectionImpl) conn)
							.getCxRequestInfo();
					if (((null == otherCxRequestInfo) && (null == cxRequestInfo))
							|| otherCxRequestInfo.equals(cxRequestInfo)) {
						return conn;
					}
				}
			}
		}
		// null is interpreted as "create new one" by the container
		return null;
	}

	public void setConnectionTracingEvents(String tracingSpec) {
		if ((null != tracingSpec) && (0 < tracingSpec.length())) {
			List<HzConnectionEvent> traceEvents = new ArrayList<HzConnectionEvent>();

			for (String traceEventId : tracingSpec.split(",")) {
				traceEventId = traceEventId.trim();
				try {
					HzConnectionEvent traceEvent = HzConnectionEvent.valueOf(traceEventId);
					if (null != traceEvent) {
						traceEvents.add(traceEvent);
					}
				} catch (IllegalArgumentException iae) {
					log(Level.WARNING, "Ignoring illegal token \""
							+ traceEventId
							+ "\" from connection config-property "
							+ "connectionTracingEvents, valid tokens are "
							+ EnumSet.allOf(HzConnectionEvent.class));
				}
			}

			this.hzConnectionTracingEvents = EnumSet.copyOf(traceEvents);
		} else {
			this.hzConnectionTracingEvents = emptySet();
		}
	}

	public void setResourceAdapter(ResourceAdapter resourceAdapter)
			throws ResourceException {
		assert resourceAdapter != null;

		if (resourceAdapter instanceof ResourceAdapterImpl) {
			this.resourceAdapter = (ResourceAdapterImpl) resourceAdapter;
		} else {
			throw new ResourceException(resourceAdapter
					+ " is not the expected ResoruceAdapterImpl but "
					+ resourceAdapter.getClass());
		}
	}

	@Override
	public String toString() {
		return "hazelcast.ManagedConnectionFactoryImpl [" + id + "]";
	}

	public void setConnectionTracingDetail(boolean connectionTracingDetail) {
		this.connectionTracingDetail = connectionTracingDetail;
	}

	public boolean isConnectionTracingDetail() {
		return connectionTracingDetail;
	}

}
