/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;

import com.hazelcast.core.ItemListener;
import com.hazelcast.impl.ClusterOperation;
import static com.hazelcast.client.ProxyHelper.check;

public abstract class CollectionClientProxy<E> extends AbstractCollection<E>{
	final protected HazelcastClient client;
	final protected ProxyHelper proxyHelper;
	final protected String name;
	
	public CollectionClientProxy(HazelcastClient hazelcastClient, String name) {
		this.name = name;
		this.client = hazelcastClient;
		proxyHelper = new ProxyHelper(name, hazelcastClient);
	}

    public void destroy() {
		proxyHelper.destroy();
	}

    public Object getId() {
		return name;
	}
	
	@Override
	public Iterator<E> iterator() {
		final Collection<E> collection = proxyHelper.keys(null);
		final AbstractCollection<E> proxy = this;
		return new Iterator<E>(){
			Iterator<E> iterator = collection.iterator();
			volatile E lastRecord;
			public boolean hasNext() {
				return iterator.hasNext();
			}

			public E next() {
				lastRecord = iterator.next();
				return lastRecord;
			}

			public void remove() {
				iterator.remove();
				proxy.remove(lastRecord);
			}
		};
	}

    @Override
    public String toString() {
        return "CollectionClientProxy{" +
                "name='" + name + '\'' +
                '}';
    }

    public void addItemListener(ItemListener<E> listener, boolean includeValue) {
        check(listener);
		Packet request = proxyHelper.createRequestPacket(ClusterOperation.ADD_LISTENER, null, null);
		request.setLongValue(includeValue?1:0);
	    Call c = proxyHelper.createCall(request);
	    client.listenerManager.addListenerCall(c);

	    proxyHelper.doCall(c);
	    client.listenerManager.registerItemListener(name, listener); 
	}
	
	public void removeItemListener(ItemListener<E> listener) {
        check(listener);
		client.listenerManager.removeItemListener(name, listener);
	}
	
	@Override
	public int size() {
		return (Integer)proxyHelper.doOp(ClusterOperation.CONCURRENT_MAP_SIZE, null, null);
	}

    public void setOutRunnable(OutRunnable out) {
		proxyHelper.setOutRunnable(out);
	}

    @Override
    public int hashCode(){
        return name.hashCode();
    }
    

}
