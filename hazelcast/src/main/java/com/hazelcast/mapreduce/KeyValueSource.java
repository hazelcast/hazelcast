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

package com.hazelcast.mapreduce;

import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ISet;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.impl.ListKeyValueSource;
import com.hazelcast.mapreduce.impl.MapKeyValueSource;
import com.hazelcast.mapreduce.impl.MultiMapKeyValueSource;
import com.hazelcast.mapreduce.impl.SetKeyValueSource;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.annotation.Beta;
import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * The abstract KeyValueSource class is used to implement custom data sources for mapreduce algorithms.<br/>
 * Default shipped implementations contains KeyValueSources for Hazelcast data structures like
 * {@link com.hazelcast.core.IMap} and {@link com.hazelcast.core.MultiMap}. Custom implementations could
 * be external files, URLs or any other data source can be visualized as key-value pairs.
 *
 * @param <K> key type
 * @param <V> value type
 * @since 3.2
 */
@Beta
public abstract class KeyValueSource<K, V>
        implements Closeable {

    /**
     * This method is called before accessing the key-value pairs of this KeyValueSource
     *
     * @param nodeEngine nodeEngine of this cluster node
     * @return true if operation succeed otherwise false
     */
    public abstract boolean open(NodeEngine nodeEngine);

    /**
     * Called to request if at least one more key-value pair is available from this
     * data source. If so this method returns true otherwise it false.
     *
     * Calls to this method will change the state, more specifically if an element is found,
     * the index will be set to the found element. Subsequent calls to the key() and element()
     * methods will return that element.
     *
     * @return true if at least one more key-value pair is available from this
     * data source, false otherwise.
     */
    public abstract boolean hasNext();

    /**
     * Returns the current index' key for {@link KeyPredicate} analysis. This is called
     * to prevent a possible deserialization of unneeded values because the key is not
     * interesting for the running mapreduce algorithm.
     *
     * Calls to this method won't change state.
     *
     * @return the current index key for {@link KeyPredicate} analysis
     */
    public abstract K key();

    /**
     * Returns the current index' element
     *
     * Calls to this method won't change state.
     *
     * @return the current index element
     */
    public abstract Map.Entry<K, V> element();

    /**
     * This method need to reset all internal state as it would be a new instance at all.
     * The same instance of the KeyValueSource may be used multiple times in a row depending
     * on the internal implementation, especially when the KeyValueSource implements
     * {@link com.hazelcast.mapreduce.PartitionIdAware}.<br/>
     * If the instance is reused a sequence of {@link #reset()}, {@link #open(com.hazelcast.spi.NodeEngine)}
     * and {@link #close()} is called multiple times with the other methods between open(...) and close().
     *
     * @return true if reset was successful otherwise false
     */
    public abstract boolean reset();

    /**
     * <p>
     * If {@link #isAllKeysSupported()} returns true a call to this method has to return
     * all clusterwide available keys. If there is no chance to precollect all keys do to
     * partitioning of the data {@link #isAllKeysSupported()} must return false.<br/>
     * </p>
     * <p>
     * If this functionality is not available and {@link Job#onKeys(Object[])},
     * {@link Job#onKeys(Iterable)} or {@link Job#keyPredicate(KeyPredicate)} is used a
     * preselection of the interesting partitions / nodes is not available and the
     * overall processing speed my be degraded.
     * </p>
     * <p>
     * If {@link #isAllKeysSupported()} returns false this method throws an
     * {@link java.lang.UnsupportedOperationException}.
     * </p>
     *
     * @return a collection of all clusterwide available keys
     */
    public final Collection<K> getAllKeys() {
        if (!isAllKeysSupported()) {
            throw new UnsupportedOperationException("getAllKeys is unsupported for this KeyValueSource");
        }
        return getAllKeys0();
    }

    /**
     * <p>
     * If it is possible to collect all clusterwide available keys for this KeyValueSource
     * implementation then this method should return true.<br/>
     * If true is returned a call to {@link #getAllKeys()} must return all available keys
     * to execute a preselection of interesting partitions / nodes based on returns keys.
     * </p>
     * <p>
     * If this functionality is not available and {@link Job#onKeys(Object[])},
     * {@link Job#onKeys(Iterable)} or {@link Job#keyPredicate(KeyPredicate)} is used a
     * preselection of the interesting partitions / nodes is not available and the
     * overall processing speed my be degraded.
     * </p>
     *
     * @return true if collecting clusterwide keys is available otherwide false
     */
    public boolean isAllKeysSupported() {
        return false;
    }

    /**
     * This method is meant for overriding to implement collecting of all clusterwide available keys
     * and returning them from {@link #getAllKeys()}.
     *
     * @return a collection of all clusterwide available keys
     */
    protected Collection<K> getAllKeys0() {
        return Collections.emptyList();
    }

    /**
     * A helper method to build a KeyValueSource implementation based on the specified {@link IMap}
     *
     * @param map map to build a KeyValueSource implementation with
     * @param <K> key type of the map
     * @param <V> value type of the map
     * @return KeyValueSource implementation based on the specified map
     */
    public static <K, V> KeyValueSource<K, V> fromMap(IMap<K, V> map) {
        return new MapKeyValueSource<K, V>(map.getName());
    }

    /**
     * A helper method to build a KeyValueSource implementation based on the specified {@link MultiMap}
     *
     * @param multiMap multiMap to build a KeyValueSource implementation with
     * @param <K>      key type of the multiMap
     * @param <V>      value type of the multiMap
     * @return KeyValueSource implementation based on the specified multiMap
     */
    public static <K, V> KeyValueSource<K, V> fromMultiMap(MultiMap<K, V> multiMap) {
        return new MultiMapKeyValueSource<K, V>(multiMap.getName());
    }

    /**
     * A helper method to build a KeyValueSource implementation based on the specified {@link IList}.<br/>
     * The key returned by this KeyValueSource implementation is <b>ALWAYS</b> the name of the list itself,
     * whereas the value are the entries of list one by one. So this implementation behaves like a MultiMap
     * with a single key but multiple values.
     *
     * @param list list to build a KeyValueSource implementation with
     * @param <V>  value type of the list
     * @return KeyValueSource implementation based on the specified list
     */
    public static <V> KeyValueSource<String, V> fromList(IList<V> list) {
        return new ListKeyValueSource<V>(list.getName());
    }

    /**
     * A helper method to build a KeyValueSource implementation based on the specified {@link ISet}.<br/>
     * The key returned by this KeyValueSource implementation is <b>ALWAYS</b> the name of the set itself,
     * whereas the value are the entries of set one by one. So this implementation behaves like a MultiMap
     * with a single key but multiple values.
     *
     * @param set set to build a KeyValueSource implementation with
     * @param <V> value type of the set
     * @return KeyValueSource implementation based on the specified set
     */
    public static <V> KeyValueSource<String, V> fromSet(ISet<V> set) {
        return new SetKeyValueSource<V>(set.getName());
    }

}
