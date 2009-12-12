/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

public class ValueCollection<K, V> implements Collection<V>{
	private final EntryHolder<K, V> proxy;
	private final Set<Entry<K,V>> entrySet;

	public ValueCollection(EntryHolder<K, V> proxy, Set<Entry<K,V>> entrySet) {
		this.proxy = proxy;
		this.entrySet = entrySet;
	}
	public boolean add(V arg0) {
		throw new UnsupportedOperationException();
	}

	public boolean addAll(Collection<? extends V> arg0) {
		throw new UnsupportedOperationException();
	}

	public void clear() {
		throw new UnsupportedOperationException();
	}

	public boolean contains(Object arg0) {
		return proxy.containsValue((V)arg0);
	}

	public boolean containsAll(Collection<?> arg0) {
		for (Iterator<?> iterator = arg0.iterator(); iterator.hasNext();) {
			Object object = (Object) iterator.next();
			if(!contains(object)){
				return false;
			}
		}
		return true;
	}

	public boolean isEmpty() {
		return proxy.size()==0;
	}

	public Iterator<V> iterator() {
		return new ValueIterator<K,V>(entrySet.iterator());
	}

	public boolean remove(Object arg0) {
		throw new UnsupportedOperationException();
	}

	public boolean removeAll(Collection<?> arg0) {
		throw new UnsupportedOperationException();
	}

	public boolean retainAll(Collection<?> arg0) {
		return false;
	}

	public int size() {
		return proxy.size();
	}

	public Object[] toArray() {
		List<V> list = new ArrayList<V>();
		for (Iterator<Entry<K,V>> iterator = entrySet.iterator(); iterator.hasNext();) {
			Entry<K, V> entry = iterator.next();
			list.add(entry.getValue());
		}
		return list.toArray();
	}

	public <T> T[] toArray(T[] arg0) {
		throw new UnsupportedOperationException();
	}
	
}
