package com.hazelcast.client;

import com.hazelcast.core.IMap;
import com.hazelcast.core.Instance;

import java.util.Iterator;
import java.util.Map.Entry;

public class MapEntryIterator<K, V> implements Iterator<java.util.Map.Entry<K, V>>{
	protected final Iterator<K> it;
	protected final EntryHolder<K,V> proxy;
    protected final Instance.InstanceType instanceType;
	protected volatile Entry<K,V> lastEntry;
    K currentIteratedKey;
    V currentIteratedValue;
    boolean hasNextCalled = false;


    public MapEntryIterator(Iterator<K> it, EntryHolder<K,V> proxy, Instance.InstanceType instanceType) {
		this.it = it;
		this.proxy = proxy;
        this.instanceType = instanceType;
	}

	public boolean hasNext() {
        hasNextCalled = true;
        if(!it.hasNext()){
            return false;
        }
        currentIteratedKey = it.next();
        currentIteratedValue = proxy.get(currentIteratedKey);
        if(currentIteratedValue ==null){
            return hasNext();
        }
		return true;
	}

	public Entry<K,V> next() {
        if(!hasNextCalled){
            hasNext();
        }
        hasNextCalled = false;
		K key = this.currentIteratedKey;
		V value = this.currentIteratedValue;
		lastEntry = new MapEntry(key, value, proxy);

        return lastEntry;

		
	}

	public void remove() {
		it.remove();
		proxy.remove(lastEntry.getKey(), lastEntry.getValue());
	}
	protected class MapEntry implements Entry<K,V>{

		private K key;
		private V value;
		private EntryHolder<K, V> proxy;

		public MapEntry(K key, V value, EntryHolder<K, V> proxy) {
			this.key = key;
			this.value = value;
			this.proxy = proxy;
		}
		
		public K getKey() {
			// TODO Auto-generated method stub
			return key;
		}

		public V getValue() {
			// TODO Auto-generated method stub
			return value;
		}

		public V setValue(V arg0) {
            if(instanceType.equals(Instance.InstanceType.MULTIMAP)){
                throw new UnsupportedOperationException();
            }
			return (V) ((IMap)proxy).put(key, arg0);
		}
	};

   
}