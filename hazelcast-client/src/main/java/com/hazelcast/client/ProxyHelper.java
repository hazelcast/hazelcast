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

package com.hazelcast.client;

import static com.hazelcast.client.Serializer.toByte;
import static com.hazelcast.client.Serializer.toObject;

import java.util.Collection;
import java.util.EventListener;
import java.util.concurrent.TimeUnit;
import java.io.Serializable;

import com.hazelcast.client.impl.CollectionWrapper;
import com.hazelcast.impl.ClusterOperation;
import com.hazelcast.query.Predicate;

public class ProxyHelper {
	OutRunnable out ;
	protected String name = "";
	final protected HazelcastClient client;

	public ProxyHelper(String name, HazelcastClient client) {
		if(name!=null){
			this.name = name;
		}
		this.client = client;
	}

	public void setOutRunnable(OutRunnable out) {
		this.out = out;
	}
	
	public String getName() {
		return name.substring(2);
	}

	protected Packet callAndGetResult(Packet request) {
		Call c = createCall(request);
	    return doCall(c);
	}

	protected Packet doCall(Call c) {
		synchronized (c) {
			try {
				out.enQueue(c);
				c.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	    Exception e = c.getException();
	    if(e!=null){
	    	throw new RuntimeException(e);
	    }
	    
	    Packet response = c.getResponse();
	    
		return response;
	}	

	protected Call createCall(Packet request) {
		Call c = new Call();
	    c.setRequest(request);
		return c;
	}

	private Packet createRequestPacket() {
		Packet request = new Packet();	    
	    request.setName(name);
	    request.setThreadId((int)Thread.currentThread().getId());
		return request;
	}
	protected Packet createRequestPacket(ClusterOperation operation, byte[] key, byte[] value) {
		Packet request = createRequestPacket();    
		request.setOperation(operation);
		request.setKey(key);
		request.setValue(value);
		return request;
	}
	protected Object doOp(ClusterOperation operation, Object key, Object value) {
		Packet request = prepareRequest(operation, key, value);
	    Packet response = callAndGetResult(request);
	    return getValue(response);
	}
	
	protected Packet prepareRequest(ClusterOperation operation, Object key,
			Object value) {
		byte[] k = null;
		byte[] v = null;
		if(key!=null){
			k= toByte(key);
		}
		if(value!=null){
			v= toByte(value);
		}
		Packet request = createRequestPacket(operation, k, v);
		return request;
	}
	
	protected Object getValue(Packet response) {
		if(response.getValue()!=null){
	    	return toObject(response.getValue());
	    }
	    return null;
	}
	
	public void destroy() {
		doOp(ClusterOperation.DESTROY, null, null);
		this.client.destroy(name);
	}
	
	public <K> Collection<K> keys(Predicate predicate) {
        
        return ((CollectionWrapper<K>)doOp(ClusterOperation.CONCURRENT_MAP_ITERATE_KEYS, null, predicate)).getKeys();
	}

    static void check(Object obj) {
        if (obj == null) {
            throw new NullPointerException("Object cannot be null.");
        }
        if (!(obj instanceof Serializable)) {
            throw new IllegalArgumentException(obj.getClass().getName() + " is not Serializable.");
        }
    }

    static void check(EventListener listener) {
        if(listener==null){
            throw new  NullPointerException("Listener can not be null");
        }
    }

    static void check(long time, TimeUnit timeunit) {
        if(time<0){
            throw new IllegalArgumentException("Time can not be less than 0.");
        }
        if(timeunit==null){
            throw new NullPointerException("TimeUnit can not be null.");
        }
    }

}
