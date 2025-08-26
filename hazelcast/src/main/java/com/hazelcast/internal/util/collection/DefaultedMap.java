/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.collection;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static java.util.stream.Collectors.toMap;

/**
 * A map decorator that introduces {@link #getOrDefault(Object)} to return a
 * default value if the delegated map does not contain the specified key. It
 * differs from {@link org.apache.commons.collections.map.DefaultedMap} by <ol>
 * <li> forwarding operations that have a default implementation in {@link Map}
 *      to the delegated map, which can increase performance if the delegated
 *      map has a custom implementation, and
 * <li> using {@link #getOrDefault(Object, Object)} to implement {@link #getOrDefault(Object)}.
 *
 * @see <a href="https://github.com/hazelcast/hazelcast/pull/25269#discussion_r1320823362">
 *      DefaultedMap vs. switch</a>
 */
public class DefaultedMap<K, V> implements Map<K, V> {
    private final Map<K, V> map;
    private final V missingValuePlaceholder;
    private final Function<K, V> missingValueComputer;

    private DefaultedMap(Map<K, V> map, V missingValuePlaceholder, Function<K, V> missingValueComputer) {
        this.map = map;
        this.missingValuePlaceholder = missingValuePlaceholder;
        this.missingValueComputer = missingValueComputer;
    }

    public DefaultedMap(Map<K, V> map, V defaultValue) {
        this(map, defaultValue, null);
    }

    public V getOrDefault(Object key) {
        V value = map.getOrDefault(key, missingValuePlaceholder);
        if (value == missingValuePlaceholder && missingValueComputer != null) {
            return missingValueComputer.apply((K) key);
        }
        return value;
    }

    public <K2, V2> DefaultedMap<K2, V2> mapKeysAndValues(Function<K, K2> keyMapper, Function<V, V2> valueMapper) {
        assert missingValueComputer == null;
        return new DefaultedMap<>(map.entrySet().stream()
                        .collect(toMap(e -> keyMapper.apply(e.getKey()), e -> valueMapper.apply(e.getValue()))),
                valueMapper.apply(missingValuePlaceholder));
    }

    public static class DefaultedMapBuilder<K, V> {
        private final Map<K, V> map;
        private V missingValuePlaceholder;

        public DefaultedMapBuilder(Map<K, V> map) {
            this.map = map;
        }

        /**
         * The missing value placeholder is passed to {@link #getOrDefault(Object, Object)} to
         * avoid querying the map twice. It is null by default and needs to be changed if the
         * map contains null values.
         */
        public DefaultedMapBuilder<K, V> missingValuePlaceholder(V missingValuePlaceholder) {
            this.missingValuePlaceholder = missingValuePlaceholder;
            return this;
        }

        public DefaultedMap<K, V> orElse(V defaultValue) {
            return new DefaultedMap<>(map, defaultValue);
        }

        public DefaultedMap<K, V> orElseGet(Function<K, V> missingValueComputer) {
            return new DefaultedMap<>(map, missingValuePlaceholder, missingValueComputer);
        }

        public DefaultedMap<K, V> orElseThrow(Function<K, Throwable> keyNotFoundException) {
            return new DefaultedMap<>(map, missingValuePlaceholder, key -> {
                throw rethrow(keyNotFoundException.apply(key));
            });
        }
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    @Override
    public V get(Object key) {
        return map.get(key);
    }

    @Override
    public V put(K key, V value) {
        return map.put(key, value);
    }

    @Override
    public V remove(Object key) {
        return map.remove(key);
    }

    @Override
    public void putAll(@Nonnull Map<? extends K, ? extends V> m) {
        map.putAll(m);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Nonnull
    @Override
    public Set<K> keySet() {
        return map.keySet();
    }

    @Nonnull
    @Override
    public Collection<V> values() {
        return map.values();
    }

    @Nonnull
    @Override
    public Set<Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    @Override
    public V getOrDefault(Object key, V defaultValue) {
        return map.getOrDefault(key, defaultValue);
    }

    @Override
    public void forEach(BiConsumer<? super K, ? super V> action) {
        map.forEach(action);
    }

    @Override
    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
        map.replaceAll(function);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return map.putIfAbsent(key, value);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return map.remove(key, value);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return map.replace(key, oldValue, newValue);
    }

    @Override
    public V replace(K key, V value) {
        return map.replace(key, value);
    }

    @Override
    public V computeIfAbsent(K key, @Nonnull Function<? super K, ? extends V> mappingFunction) {
        return map.computeIfAbsent(key, mappingFunction);
    }

    @Override
    public V computeIfPresent(K key, @Nonnull BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return map.computeIfPresent(key, remappingFunction);
    }

    @Override
    public V compute(K key, @Nonnull BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return map.compute(key, remappingFunction);
    }

    @Override
    public V merge(K key, @Nonnull V value, @Nonnull BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        return map.merge(key, value, remappingFunction);
    }
}
