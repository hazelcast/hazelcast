/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache;

import com.hazelcast.cache.impl.event.CachePartitionLostListener;
import com.hazelcast.core.PrefixedDistributedObject;

import javax.cache.Cache;
import javax.cache.expiry.ExpiryPolicy;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ForkJoinPool;

/**
 * This {@link com.hazelcast.cache.ICache} interface is the {@link javax.cache.Cache} extension offered by
 * Hazelcast JCache.<br>
 * In addition to the standard set of JCache methods defined in the JSR 107 specification, Hazelcast provides
 * additional operations to support a broader range of programing styles.<p>
 *
 * There are three different types of extensions methods provided:
 * <ul>
 * <li>asynchronous version of typical blocking cache operations (due to remote calls),</li>
 * <li>typical cache operations, providing a custom {@link javax.cache.expiry.ExpiryPolicy} parameter
 * to apply a special expiration to that specific operation, and</li>
 * <li>common collection-like operations (e.g. {@link #size()}) or typical Hazelcast-list additions
 * (e.g. {@link #destroy()}).</li>
 * </ul><p>
 *
 * To take advantage of the methods of this interface, the {@link javax.cache.Cache} instance needs to be
 * unwrapped as defined in the JSR 107 standard ({@link javax.cache.Cache#unwrap(Class)}) by providing the
 * {@link com.hazelcast.cache.ICache} interface parameter.
 * <pre>
 *   ICache&lt;Key , Value&gt; unwrappedCache =  cache.unwrap( ICache.class );
 * </pre>
 * The unwrapped cache instance can now be used for both ICache and Cache operations.<p>
 *
 * <b>Asynchronous operations:</b><br>
 * For most of the typical operations, Hazelcast provides asynchronous versions to program in a more reactive
 * styled way. All asynchronous operations follow the same naming pattern: the operation's name from JCache
 * extended by the term <code>Async</code>; for example, the asynchronous version of {@link javax.cache.Cache#get(Object)}
 * is {@link #getAsync(Object)}.<br>
 * These methods return a {@link CompletionStage} that can be used to chain further computation stages.
 * Alternatively, a {@link java.util.concurrent.CompletableFuture} can be obtained
 * via {@link CompletionStage#toCompletableFuture()} to wait for the operation to be completed in a
 * blocking fashion with {@link java.util.concurrent.Future#get()} or
 * {@link java.util.concurrent.Future#get(long, java.util.concurrent.TimeUnit)}.<p>
 * In a reactive way:
 * <pre>
 *   CompletionStage&lt;Value&gt; stage = unwrappedCache.getAsync( &quot;key-1&quot; ) ;
 *   stage.thenAcceptAsync(value -&gt; {
 *         System.out.println(value);
 *     });
 * </pre>
 * Or in a blocking way:
 * <pre>
 *   CompletionStage&lt;Value&gt; stage = unwrappedCache.getAsync( &quot;key-1&quot; ) ;
 *   Value value = stage.toCompletableFuture().get();
 *   System.out.println( value );
 * </pre>
 *
 * <b>Execution properties of returned {@code CompletionStage}s</b>
 * <p>
 *     Actions supplied for dependent completions of non-async methods and async methods
 *     without an explicit {@link java.util.concurrent.Executor} argument are performed
 *     by the {@link ForkJoinPool#commonPool()} (unless it does not support a parallelism
 *     level of at least 2, in which case a new {@code Thread} is created per task).
 * </p>
 *
 *
 * <p>
 * Dependent actions can be registered on the returned {@link CompletionStage}.
 * Their execution follows {@link java.util.concurrent.CompletableFuture}
 * execution conventions:
 * <ul>
 *     <li>dependent actions registered by <em>non-async</em> methods may
 *     be executed by the thread that completes the current CompletableFuture or by any other
 *     caller of a completion method.</li>
 *     <li>dependent actions registered by default <em>async</em> methods without an explicit
 *     {@code Executor} argument are executed by {@link ForkJoinPool#commonPool()} unless
 *     its parallelism level is less than 2, in which case a new {@code Thread} is created to
 *     run each action</li>
 * </ul>
 *
 * <b>Custom ExpiryPolicy:</b><br>
 * For most of the typical operations, Hazelcast provides overloaded versions with an additional
 * {@link javax.cache.expiry.ExpiryPolicy} parameter to configure a different expiration policy from the
 * default one set in the {@link javax.cache.configuration.CompleteConfiguration} passed to the cache
 * creation. Therefore the {@link javax.cache.Cache#put(Object, Object)} operation has an overload
 * {@link com.hazelcast.cache.ICache#put(Object, Object, javax.cache.expiry.ExpiryPolicy)} to pass in the
 * special policy.<p>
 * <b><i>Important: The overloads use an instance of {@link javax.cache.expiry.ExpiryPolicy}, not
 * a {@link javax.cache.configuration.Factory} instance as is used in the configuration.</i></b>
 * <pre>
 *   unwrappedCache.put( "key", "value", new AccessedExpiryPolicy( Duration.ONE_DAY ) );
 * </pre>
 *
 * <p>
 * <b>Split-brain</b>
 * <p>
 * Behaviour of {@link ICache} under split-brain scenarios should be taken into account when using this
 * data structure.  During a split, each partitioned cluster will either create a brand new {@link ICache}
 * or it will continue to use the primary or back-up version.
 * <p>
 * As a defensive mechanism against such inconsistency, consider using the in-built
 * <a href="http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#split-brain-protection">
 * split-brain protection for {@link ICache}</a>.  Using this functionality it is possible to restrict operations in smaller
 * partitioned clusters. It should be noted that there is still an inconsistency window between the time of
 * the split and the actual detection. Therefore using this reduces the window of inconsistency but can never
 * completely eliminate it.
 * <p>
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 * @see javax.cache.Cache
 * @since 3.3.1
 */
public interface ICache<K, V>
        extends javax.cache.Cache<K, V>, PrefixedDistributedObject {


    /**
     * Associates the specified key with the given {@link javax.cache.expiry.ExpiryPolicy}.
     * {@code expiryPolicy} takes precedence for this particular {@code key} against any cache wide expiry policy.
     * If {@code key} does not exist or is already expired, this call makes no changes to entries stored in this cache.
     *
     * Note: New time-to-live duration is calculated using newly added entry policy's getExpiryForUpdate method
     * immediately after this operation succeeds.
     *
     * @param   key The key that is associated with the specified expiry policy.
     * @param   expiryPolicy custom expiry policy for this operation
     * @return {@code true} if the entry exists and its expiry policy is changed, {@code false} otherwise
     * @throws  NullPointerException if {@code keys} or {@code expiryPolicy} is null.
     * @since 3.11
     */
    boolean setExpiryPolicy(K key, ExpiryPolicy expiryPolicy);

    /**
     * Associates the specified key with the given {@link javax.cache.expiry.ExpiryPolicy}.
     * {@code expiryPolicy} takes precedence for these particular {@code keys} against any cache wide expiry policy.
     * If some keys in {@code keys} do not exist or are already expired, this call has no effect for those.
     *
     * Note: New time-to-live duration is calculated using newly added entry policy's getExpiryForUpdate method
     * immediately after this operation succeeds.
     *
     * @param   keys The keys that are associated with the specified expiry policy.
     * @param   expiryPolicy custom expiry policy for this operation
     * @throws  NullPointerException if {@code keys} or {@code expiryPolicy} is null.
     * @since 3.11
     */
    void setExpiryPolicy(Set<? extends K> keys, ExpiryPolicy expiryPolicy);

    /**
     * Asynchronously retrieves the mapped value of the given key using a custom
     * {@link javax.cache.expiry.ExpiryPolicy}. If no mapping exists <code>null</code> is returned.
     * <p>
     * If the cache is configured for <code>read-through</code> operation mode, the underlying
     * configured {@link javax.cache.integration.CacheLoader} might be called to retrieve
     * the value of the key from any kind of external resource.
     * <p>
     * The resulting {@link CompletionStage} instance may throw a
     * {@link java.lang.ClassCastException} as the operations result if the {@link javax.cache.Cache}
     * is configured to perform runtime-type-checking, and the key or value types are incompatible
     * with those that have been configured for the {@link javax.cache.Cache}.
     *
     * @param key The key whose associated value is to be returned.
     * @return CompletionStage retrieve the value assigned to the given key.
     * @throws java.lang.NullPointerException if given key is null
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}
     * @see javax.cache.Cache#get(Object)
     */
    CompletionStage<V> getAsync(K key);

    /**
     * Asynchronously gets an entry from cache using a custom {@link javax.cache.expiry.ExpiryPolicy}.
     * <p>
     * If the cache is configured for <code>read-through</code> operation mode, the underlying
     * configured {@link javax.cache.integration.CacheLoader} might be called to retrieve
     * the value of the key from any kind of external resource.
     * <p>
     * The resulting {@link CompletionStage} instance may throw a
     * {@link java.lang.ClassCastException} as the operations result if the {@link javax.cache.Cache}
     * is configured to perform runtime-type-checking, and the key or value types are incompatible
     * with those that have been configured for the {@link javax.cache.Cache}.
     *
     * @param key          The key whose associated value is to be returned.
     * @param expiryPolicy The custom expiry policy for this operation,
     *                     a null value is equivalent to {@link #getAsync(Object)}.
     * @return CompletionStage retrieve the value assigned to the given key.
     * @throws java.lang.NullPointerException if given key is null.
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}.
     * @see javax.cache.Cache#get(Object)
     */
    CompletionStage<V> getAsync(K key, ExpiryPolicy expiryPolicy);

    /**
     * Asynchronously associates the specified value with the specified key in the cache.
     * <p>
     * In case a previous assignment already exists, the previous value is overridden by
     * the new given value.
     * <p>
     * If the cache is configured for <code>write-through</code> operation mode, the underlying
     * configured {@link javax.cache.integration.CacheWriter} might be called to store
     * the value of the key to any kind of external resource.
     *
     * @param key   The key whose associated value is to be returned.
     * @param value The value to be associated with the specified key.
     * @return CompletionStage notify when the operation succeeds.
     * @throws java.lang.NullPointerException if the given key or value is null.
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}.
     * @throws ClassCastException             if the implementation is configured to perform
     *                                        runtime-type-checking, and the key or value
     *                                        types are incompatible with those that have been
     *                                        configured for the {@link javax.cache.Cache}.
     * @see javax.cache.Cache#put(Object, Object)
     */
    CompletionStage<Void> putAsync(K key, V value);

    /**
     * Asynchronously associates the specified value with the specified key in the cache using
     * a custom {@link javax.cache.expiry.ExpiryPolicy}.
     * <p>
     * In case a previous assignment already exists, the previous value is overridden by
     * the new given value.
     * <p>
     * If the cache is configured for <code>write-through</code> operation mode, the underlying
     * configured {@link javax.cache.integration.CacheWriter} might be called to store
     * the value of the key to any kind of external resource.
     *
     * @param key          The key whose associated value is to be returned.
     * @param value        The value to be associated with the specified key.
     * @param expiryPolicy The custom expiry policy for this operation,
     *                     a null value is equivalent to {@link #putAsync(Object, Object)}.
     * @return CompletionStage notify when the operation succeeds.
     * @throws java.lang.NullPointerException if the given key or value is null.
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}.
     * @throws ClassCastException             if the implementation is configured to perform
     *                                        runtime-type-checking, and the key or value
     *                                        types are incompatible with those that have been
     *                                        configured for the {@link javax.cache.Cache}.
     * @see javax.cache.Cache#put(Object, Object)
     */
    CompletionStage<Void> putAsync(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * Asynchronously associates the specified key with the given value if and only if there is not yet
     * a mapping defined for the specified key.
     * <p>
     * This is equivalent to:
     * <pre>
     *   if (!cache.containsKey(key)) {}
     *     cache.put(key, value);
     *     return true;
     *   } else {
     *     return false;
     *   }
     * </pre>
     * except that the action is performed atomically.
     * <p>
     * If the cache is configured for <code>write-through</code> operation mode, the underlying
     * configured {@link javax.cache.integration.CacheWriter} might be called to store
     * the value of the key to any kind of external resource.
     *
     * @param key   The key that is associated with the specified value.
     * @param value The value to which the specified key is associated.
     * @return CompletionStage notify if a previous value was assigned with the key
     * @throws java.lang.NullPointerException if the given key or value is null
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}
     * @throws ClassCastException             if the implementation is configured to perform
     *                                        runtime-type-checking, and the key or value
     *                                        types are incompatible with those that have been
     *                                        configured for the {@link javax.cache.Cache}
     * @see javax.cache.Cache#putIfAbsent(Object, Object)
     */
    CompletionStage<Boolean> putIfAbsentAsync(K key, V value);

    /**
     * Asynchronously associates the specified key with the given value if and only if there is not yet
     * a mapping defined for the specified key.
     * using a custom {@link javax.cache.expiry.ExpiryPolicy}.
     * <p>
     * This is equivalent to:
     * <pre>
     *   if (!cache.containsKey(key)) {}
     *     cache.put(key, value);
     *     return true;
     *   } else {
     *     return false;
     *   }
     * </pre>
     * except that the action is performed atomically.
     * <p>
     * If the cache is configured for <code>write-through</code> operation mode, the underlying
     * configured {@link javax.cache.integration.CacheWriter} might be called to store
     * the value of the key to any kind of external resource.
     *
     * @param key          The key that is associated with the specified value.
     * @param value        The value to which the specified key is associated.
     * @param expiryPolicy custom expiry policy for this operation,
     *                     a null value is equivalent to
     *                     {@link #putIfAbsentAsync(Object, Object)}
     * @return CompletionStage notify if a previous value was assigned with the key
     * @throws java.lang.NullPointerException if the given key or value is null
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}
     * @throws ClassCastException             if the implementation is configured to perform
     *                                        runtime-type-checking, and the key or value
     *                                        types are incompatible with those that have been
     *                                        configured for the {@link javax.cache.Cache}
     * @see javax.cache.Cache#putIfAbsent(Object, Object)
     */
    CompletionStage<Boolean> putIfAbsentAsync(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * Asynchronously associates the specified value with the specified key in this cache,
     * returning an existing value if one existed.
     * <p>
     * In case a previous assignment already exists, the previous value is overridden by
     * the new given value and the previous value is returned to the caller. This is
     * equivalent to the {@link java.util.Map#put(Object, Object)} operation.
     * <p>
     * If the cache is configured for <code>write-through</code> operation mode, the underlying
     * configured {@link javax.cache.integration.CacheWriter} might be called to store
     * the value of the key to any kind of external resource.
     *
     * @param key   The key whose associated value is to be returned.
     * @param value The value that is associated with the specified key.
     * @return CompletionStage retrieve a possible previously assigned value for the given key.
     * @throws java.lang.NullPointerException if the given key or value is null.
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}.
     * @throws ClassCastException             if the implementation is configured to perform
     *                                        runtime-type-checking, and the key or value
     *                                        types are incompatible with those that have been
     *                                        configured for the {@link javax.cache.Cache}.
     * @see javax.cache.Cache#getAndPut(Object, Object)
     */
    CompletionStage<V> getAndPutAsync(K key, V value);

    /**
     * Asynchronously associates the specified value with the specified key in this cache,
     * returning an existing value if one existed using a custom {@link javax.cache.expiry.ExpiryPolicy}.
     * <p>
     * In case a previous assignment already exists, the previous value is overridden by
     * the new given value and the previous value is returned to the caller. This is
     * equivalent to the {@link java.util.Map#put(Object, Object)} operation.
     * <p>
     * If the cache is configured for <code>write-through</code> operation mode, the underlying
     * configured {@link javax.cache.integration.CacheWriter} might be called to store
     * the value of the key to any kind of external resource.
     *
     * @param key          The key whose associated value is to be returned.
     * @param value        The value to associate with the specified key.
     * @param expiryPolicy The custom expiry policy for this operation,
     *                     a null value is equivalent to {@link #getAndPutAsync(Object, Object)}.
     * @return CompletionStage retrieve a possible previously assigned value for the given key.
     * @throws java.lang.NullPointerException if the given key or value is null.
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}.
     * @throws ClassCastException             if the implementation is configured to perform
     *                                        runtime-type-checking, and the key or value
     *                                        types are incompatible with those that have been
     *                                        configured for the {@link javax.cache.Cache}.
     * @see javax.cache.Cache#getAndPut(Object, Object)
     */
    CompletionStage<V> getAndPutAsync(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * Asynchronously removes the mapping for a key from this cache if it is present.
     * <p>
     * If the cache is configured for <code>write-through</code> operation mode, the underlying
     * configured {@link javax.cache.integration.CacheWriter} might be called to store
     * the value of the key to any kind of external resource.
     * <p>
     * The resulting {@link CompletionStage} instance may throw a
     * {@link java.lang.ClassCastException} as the operations result if the {@link javax.cache.Cache}
     * is configured to perform runtime-type-checking, and the key or value types are incompatible
     * with those that have been configured for the {@link javax.cache.Cache}.
     *
     * @param key The key whose mapping is to be removed.
     * @return CompletionStage notify if mapping could be removed or not.
     * @throws java.lang.NullPointerException if the given key is null.
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}.
     * @see javax.cache.Cache#remove(Object)
     */
    CompletionStage<Boolean> removeAsync(K key);

    /**
     * Asynchronously removes the mapping for the given key if and only if the
     * currently mapped value equals to the value of <code>oldValue</code>.
     * <p>
     * This is equivalent to:
     * <pre>
     *   if (cache.containsKey(key) &amp;&amp; equals(cache.get(key), oldValue) {
     *     cache.remove(key);
     *     return true;
     *   } else {
     *     return false;
     *   }
     * </pre>
     * except that the action is performed atomically.
     * <p>
     * If the cache is configured for <code>write-through</code> operation mode, the underlying
     * configured {@link javax.cache.integration.CacheWriter} might be called to store
     * the value of the key to any kind of external resource.
     * <p>
     * The resulting {@link CompletionStage} instance may throw a
     * {@link java.lang.ClassCastException} as the operations result if the {@link javax.cache.Cache}
     * is configured to perform runtime-type-checking, and the key or value types are incompatible
     * with those that have been configured for the {@link javax.cache.Cache}.
     *
     * @param key      The key whose mapping is to be removed if the mapped value is oldValue.
     * @param oldValue The value expected to be associated with the specified key.
     * @return CompletionStage notify if mapping could be removed or not.
     * @throws java.lang.NullPointerException if the given key or oldValue is null.
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}.
     * @see javax.cache.Cache#remove(Object, Object)
     */
    CompletionStage<Boolean> removeAsync(K key, V oldValue);

    /**
     * Asynchronously removes the entry for a key and returns the previously assigned value or null
     * if no value was assigned.
     * <p>
     * If the cache is configured for <code>write-through</code> operation mode, the underlying
     * configured {@link javax.cache.integration.CacheWriter} might be called to store
     * the value of the key to any kind of external resource.
     * <p>
     * The resulting {@link CompletionStage} instance may throw a
     * {@link java.lang.ClassCastException} as the operations result if the {@link javax.cache.Cache}
     * is configured to perform runtime-type-checking, and the key or value types are incompatible
     * with those that have been configured for the {@link javax.cache.Cache}.
     *
     * @param key The key to be removed and whose associated value is to be returned.
     * @return CompletionStage retrieve a possible previously assigned value for the removed key.
     * @throws java.lang.NullPointerException if the given key is null.
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}.
     * @see javax.cache.Cache#getAndRemove(Object)
     */
    CompletionStage<V> getAndRemoveAsync(K key);

    /**
     * Asynchronously replaces the assigned value of the given key by the specified value.
     * <p>
     * If the cache is configured for <code>write-through</code> operation mode, the underlying
     * configured {@link javax.cache.integration.CacheWriter} might be called to store
     * the value of the key to any kind of external resource.
     *
     * @param key   The key whose associated value is to be replaced.
     * @param value The new value to be associated with the specified key.
     * @return CompletionStage notify if the operation succeeds or not.
     * @throws java.lang.NullPointerException if the given key or value is null.
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}.
     * @throws ClassCastException             if the implementation is configured to perform
     *                                        runtime-type-checking, and the key or value
     *                                        types are incompatible with those that have been
     *                                        configured for the {@link javax.cache.Cache}.
     * @see javax.cache.Cache#replace(Object, Object)
     */
    CompletionStage<Boolean> replaceAsync(K key, V value);

    /**
     * Asynchronously replaces the assigned value of the given key by the specified value
     * using a custom {@link javax.cache.expiry.ExpiryPolicy}.
     * <p>
     * If the cache is configured for <code>write-through</code> operation mode, the underlying
     * configured {@link javax.cache.integration.CacheWriter} might be called to store
     * the value of the key to any kind of external resource.
     *
     * @param key          The key whose assigned value is replaced by the specified value.
     * @param value        The specified value to be associated with the given key.
     * @param expiryPolicy The custom expiry policy for this operation,
     *                     a null value is equivalent to {@link #replaceAsync(Object, Object)}
     * @return CompletionStage notify if the operation succeeds or not.
     * @throws java.lang.NullPointerException if the given key or value is null.
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}.
     * @throws ClassCastException             if the implementation is configured to perform
     *                                        runtime-type-checking, and the key or value
     *                                        types are incompatible with those that have been
     *                                        configured for the {@link javax.cache.Cache}.
     * @see javax.cache.Cache#replace(Object, Object)
     */
    CompletionStage<Boolean> replaceAsync(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * Asynchronously replaces the currently assigned value for the given key with the specified
     * <code>newValue</code> if and only if the currently assigned value equals the value of
     * <code>oldValue</code>.
     * <p>
     * This is equivalent to:
     * <pre>
     *   if (cache.containsKey(key) &amp;&amp; equals(cache.get(key), oldValue) {
     *     cache.put(key, newValue);
     *     return true;
     *   } else {
     *     return false;
     *   }
     * </pre>
     * except that the action is performed atomically.
     * <p>
     * If the cache is configured for <code>write-through</code> operation mode, the underlying
     * configured {@link javax.cache.integration.CacheWriter} might be called to store
     * the value of the key to any kind of external resource.
     * <p>
     * The resulting {@link CompletionStage} instance may throw a
     * {@link java.lang.ClassCastException} as the operations result if the {@link javax.cache.Cache}
     * is configured to perform runtime-type-checking, and the key or value types are incompatible
     * with those that have been configured for the {@link javax.cache.Cache}.
     *
     * @param key      The key that will have its assigned value replaced.
     * @param oldValue The old value expected to be associated with the specified key.
     * @param newValue The new value to be associated with the specified key.
     * @return CompletionStage notify if the operation succeeds or not.
     * @throws java.lang.NullPointerException if the given key, oldValue or newValue is null.
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}
     * @throws ClassCastException             if the implementation is configured to perform
     *                                        runtime-type-checking, and the key or value
     *                                        types are incompatible with those that have been
     *                                        configured for the {@link javax.cache.Cache}
     * @see javax.cache.Cache#replace(Object, Object, Object)
     */
    CompletionStage<Boolean> replaceAsync(K key, V oldValue, V newValue);

    /**
     * Asynchronously replaces the currently assigned value for the given key with the specified
     * <code>newValue</code> if and only if the currently assigned value equals the value of
     * <code>oldValue</code> using a custom {@link javax.cache.expiry.ExpiryPolicy}.
     * <p>
     * This is equivalent to:
     * <pre>
     *   if (cache.containsKey(key) &amp;&amp; equals(cache.get(key), oldValue) {
     *     cache.put(key, newValue);
     *     return true;
     *   } else {
     *     return false;
     *   }
     * </pre>
     * except that the action is performed atomically.
     * <p>
     * If the cache is configured for <code>write-through</code> operation mode, the underlying
     * configured {@link javax.cache.integration.CacheWriter} might be called to store
     * the value of the key to any kind of external resource.
     * <p>
     * The resulting {@link CompletionStage} instance may throw a
     * {@link java.lang.ClassCastException} as the operations result if the {@link javax.cache.Cache}
     * is configured to perform runtime-type-checking, and the key or value types are incompatible
     * with those that have been configured for the {@link javax.cache.Cache}.
     *
     * @param key          The key that will have its assigned value replaced.
     * @param oldValue     The old value expected to be associated with the specified key.
     * @param newValue     The new value to be associated with the specified key.
     * @param expiryPolicy The custom expiry policy for this operation,
     *                     a null value is equivalent to {@link #replaceAsync(Object, Object, Object)}.
     * @return CompletionStage to get notified if the operation succeed or not.
     * @throws java.lang.NullPointerException if the given key, oldValue or newValue is null.
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}.
     * @throws ClassCastException             if the implementation is configured to perform
     *                                        runtime-type-checking, and the key or value
     *                                        types are incompatible with those that have been
     *                                        configured for the {@link javax.cache.Cache}.
     * @see javax.cache.Cache#replace(Object, Object, Object)
     */
    CompletionStage<Boolean> replaceAsync(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy);

    /**
     * Asynchronously replaces the assigned value of the given key by the specified value and returns
     * the previously assigned value.
     * <p>
     * If the cache is configured for <code>write-through</code> operation mode, the underlying
     * configured {@link javax.cache.integration.CacheWriter} might be called to store
     * the value of the key to any kind of external resource.
     *
     * @param key   The key whose value is replaced.
     * @param value The new value to be associated with the specified key.
     * @return CompletionStage to retrieve a possible previously assigned value for the given key.
     * @throws java.lang.NullPointerException if the given key or value is null.
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}.
     * @throws ClassCastException             if the implementation is configured to perform
     *                                        runtime-type-checking, and the key or value
     *                                        types are incompatible with those that have been
     *                                        configured for the {@link javax.cache.Cache}.
     * @see javax.cache.Cache#getAndReplace(Object, Object)
     */
    CompletionStage<V> getAndReplaceAsync(K key, V value);

    /**
     * Asynchronously replaces the assigned value of the given key by the specified value using a
     * custom {@link javax.cache.expiry.ExpiryPolicy} and returns the previously assigned value.
     * <p>
     * If the cache is configured for <code>write-through</code> operation mode, the underlying
     * configured {@link javax.cache.integration.CacheWriter} might be called to store
     * the value of the key to any kind of external resource.
     *
     * @param key          The key whose value is replaced.
     * @param value        The new value to be associated with the specified key.
     * @param expiryPolicy The custom expiry policy for this operation,
     *                     a null value is equivalent to {@link #getAndReplace(Object, Object)}
     * @return CompletionStage to retrieve a possible previously assigned value for the given key.
     * @throws java.lang.NullPointerException if the given key or value is null.
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}.
     * @throws ClassCastException             if the implementation is configured to perform
     *                                        runtime-type-checking, and the key or value
     *                                        types are incompatible with those that have been
     *                                        configured for the {@link javax.cache.Cache}.
     * @see javax.cache.Cache#getAndReplace(Object, Object)
     */
    CompletionStage<V> getAndReplaceAsync(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * Retrieves the mapped value of the given key using a custom {@link javax.cache.expiry.ExpiryPolicy}.
     * If no mapping exists <code>null</code> is returned.
     * <p>
     * If the cache is configured for <code>read-through</code> operation mode, the underlying
     * configured {@link javax.cache.integration.CacheLoader} might be called to retrieve
     * the value of the key from any kind of external resource.
     *
     * @param key          The key whose mapped value is to be returned.
     * @param expiryPolicy The custom expiry policy for this operation,
     *                     a null value is equivalent to {@link #get(Object)}.
     * @return The value assigned to the given key, or null if not assigned.
     * @throws java.lang.NullPointerException if the given key is null.
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}.
     * @throws ClassCastException             if the implementation is configured to perform
     *                                        runtime-type-checking, and the key or value
     *                                        types are incompatible with those that have been
     *                                        configured for the {@link javax.cache.Cache}
     * @see javax.cache.Cache#get(Object)
     */
    V get(K key, ExpiryPolicy expiryPolicy);

    /**
     * Gets a collection of entries from the cache with custom expiry policy, returning them as
     * {@link Map} of the values associated with the set of keys requested.
     * <p>
     * If the cache is configured for <code>read-through</code> operation mode, the underlying
     * configured {@link javax.cache.integration.CacheLoader} might be called to retrieve
     * the values of the keys from any kind of external resource.
     *
     * @param keys         The keys whose associated values are to be returned.
     * @param expiryPolicy The custom expiry policy for this operation,
     *                     a null value is equivalent to {@link #getAll(java.util.Set)}.
     * @return A map of entries that were found for the given keys. Keys not found
     * in the cache are not in the returned map.
     * @throws java.lang.NullPointerException if the given keys are null.
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}
     * @throws ClassCastException             if the implementation is configured to perform
     *                                        runtime-type-checking, and the key or value
     *                                        types are incompatible with those that have been
     *                                        configured for the {@link javax.cache.Cache}.
     * @see javax.cache.Cache#getAll(java.util.Set)
     */
    Map<K, V> getAll(Set<? extends K> keys, ExpiryPolicy expiryPolicy);

    /**
     * Associates the specified value with the specified key in the cache using a custom {@link javax.cache.expiry.ExpiryPolicy}.
     *
     * @param key          The key that has the specified value associated with it.
     * @param value        The value to be associated with the key.
     * @param expiryPolicy The custom expiry policy for this operation,
     *                     a null value is equivalent to {@link #put(Object, Object)}.
     * @throws java.lang.NullPointerException if the given key or value is null.
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}.
     * @throws ClassCastException             if the implementation is configured to perform
     *                                        runtime-type-checking, and the key or value
     *                                        types are incompatible with those that have been
     *                                        configured for the {@link javax.cache.Cache}.
     * @see javax.cache.Cache#put(Object, Object)
     */
    void put(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * Associates the specified value with the specified key in this cache using a custom {@link javax.cache.expiry.ExpiryPolicy},
     * returning an existing value if one existed.
     *
     * @param key          The key that has the specified value associated with it.
     * @param value        The value to be associated with the key.
     * @param expiryPolicy The custom expiry policy for this operation,
     *                     a null value is equivalent to {@link #getAndPut(Object, Object)}.
     * @return The value previously assigned to the given key, or null if not assigned.
     * @throws java.lang.NullPointerException if the given key or value is null.
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}.
     * @throws ClassCastException             if the implementation is configured to perform
     *                                        runtime-type-checking, and the key or value
     *                                        types are incompatible with those that have been
     *                                        configured for the {@link javax.cache.Cache}.
     * @see javax.cache.Cache#getAndPut(Object, Object)
     */
    V getAndPut(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * Copies all of the entries from the given map to the cache using a custom
     * {@link javax.cache.expiry.ExpiryPolicy}.
     * <p>
     * Puts of single entries happen atomically but there is no transactional guarantee over
     * the complete <code>putAll</code> operation. If other concurrent operations modify or remove
     * all or single values of the provided map, the result is undefined.
     * <p>
     * If the cache is configured for <code>write-through</code> operation mode, the underlying
     * configured {@link javax.cache.integration.CacheWriter} might be called to store
     * the values of the keys to any kind of external resource.
     *
     * @param map          The mappings to be stored in this cache.
     * @param expiryPolicy The custom expiry policy for this operation,
     *                     a null value is equivalent to {@link #putAll(java.util.Map)}.
     * @throws java.lang.NullPointerException if the given map is null.
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}.
     * @throws ClassCastException             if the implementation is configured to perform
     *                                        runtime-type-checking, and the key or value
     *                                        types are incompatible with those that have been
     *                                        configured for the {@link javax.cache.Cache}.
     * @see javax.cache.Cache#putAll(java.util.Map)
     */
    void putAll(java.util.Map<? extends K, ? extends V> map, ExpiryPolicy expiryPolicy);

    /**
     * Associates the specified key with the given value if and only if there is not yet
     * a mapping defined for the specified key.
     * <p>
     * This is equivalent to:
     * <pre>
     *   if (!cache.containsKey(key)) {}
     *     cache.put(key, value);
     *     return true;
     *   } else {
     *     return false;
     *   }
     * </pre>
     * except that the action is performed atomically.
     * <p>
     * If the cache is configured for <code>write-through</code> operation mode, the underlying
     * configured {@link javax.cache.integration.CacheWriter} might be called to store
     * the value of the key to any kind of external resource.
     *
     * @param key          The key that is associated with the specified value.
     * @param value        The value that has the specified key associated with it.
     * @param expiryPolicy The custom expiry policy for this operation,
     *                     a null value is equivalent to {@link #putIfAbsent(Object, Object)}.
     * @return true if a value was set, false otherwise.
     * @throws java.lang.NullPointerException if the given key or value is null.
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}.
     * @throws ClassCastException             if the implementation is configured to perform
     *                                        runtime-type-checking, and the key or value
     *                                        types are incompatible with those that have been
     *                                        configured for the {@link javax.cache.Cache}.
     * @see javax.cache.Cache#putIfAbsent(Object, Object)
     */
    boolean putIfAbsent(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * Atomically replaces the currently assigned value for the given key with the specified
     * <code>newValue</code> if and only if the currently assigned value equals the value of
     * <code>oldValue</code> using a custom {@link javax.cache.expiry.ExpiryPolicy}.
     * <p>
     * This is equivalent to:
     * <pre>
     *   if (cache.containsKey(key) &amp;&amp; equals(cache.get(key), oldValue) {
     *     cache.put(key, newValue);
     *     return true;
     *   } else {
     *     return false;
     *   }
     * </pre>
     * except that the action is performed atomically.
     * <p>
     * If the cache is configured for <code>write-through</code> operation mode, the underlying
     * configured {@link javax.cache.integration.CacheWriter} might be called to store
     * the value of the key to any kind of external resource.
     *
     * @param key          The key with the value to be replaced.
     * @param oldValue     The old value expected to be associated with the specified key.
     * @param newValue     The new value to be associated with the specified key.
     * @param expiryPolicy The custom expiry policy for this operation,
     *                     a null value is equivalent to {@link #replace(Object, Object, Object)}.
     * @return true if a value was replaced, false otherwise.
     * @throws java.lang.NullPointerException if given key, oldValue or newValue is null
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}.
     * @throws ClassCastException             if the implementation is configured to perform
     *                                        runtime-type-checking, and the key or value
     *                                        types are incompatible with those that have been
     *                                        configured for the {@link javax.cache.Cache}.
     * @see javax.cache.Cache#replace(Object, Object, Object)
     */
    boolean replace(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy);

    /**
     * Atomically replaces the assigned value of the given key by the specified value
     * using a custom {@link javax.cache.expiry.ExpiryPolicy}.
     * <p>
     * If the cache is configured for <code>write-through</code> operation mode, the underlying
     * configured {@link javax.cache.integration.CacheWriter} might be called to store
     * the value of the key to any kind of external resource.
     *
     * @param key          The key whose value is replaced.
     * @param value        The new value to be associated with the specified key.
     * @param expiryPolicy The custom expiry policy for this operation,
     *                     a null value is equivalent to {@link #replace(Object, Object)}
     * @return true if a value was replaced, false otherwise.
     * @throws java.lang.NullPointerException if the given key, oldValue or newValue is null.
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}.
     * @throws ClassCastException             if the implementation is configured to perform
     *                                        runtime-type-checking, and the key or value
     *                                        types are incompatible with those that have been
     *                                        configured for the {@link javax.cache.Cache}.
     * @see javax.cache.Cache#replace(Object, Object)
     */
    boolean replace(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * Atomically replaces the assigned value of the given key by the specified value using a
     * custom {@link javax.cache.expiry.ExpiryPolicy} and returns the previously assigned value.
     * <p>
     * If the cache is configured for <code>write-through</code> operation mode, the underlying
     * configured {@link javax.cache.integration.CacheWriter} might be called to store
     * the value of the key to any kind of external resource.
     *
     * @param key          The key whose value is replaced.
     * @param value        The new value to be associated with the specified key.
     * @param expiryPolicy The custom expiry policy for this operation,
     *                     a null value is equivalent to {@link #getAndReplace(Object, Object)}.
     * @return The old value previously assigned to the given key.
     * @throws java.lang.NullPointerException if the given key or value is null.
     * @throws javax.cache.CacheException     if any exception
     *                                        happens while invoking the request, other exceptions are wrapped.
     * @throws IllegalStateException          if the cache is {@link #isClosed()}.
     * @throws ClassCastException             if the implementation is configured to perform
     *                                        runtime-type-checking, and the key or value
     *                                        types are incompatible with those that have been
     *                                        configured for the {@link javax.cache.Cache}.
     * @see javax.cache.Cache#getAndReplace(Object, Object)
     */
    V getAndReplace(K key, V value, ExpiryPolicy expiryPolicy);

    /**
     * Total entry count. If the cache contains more than
     * <code>Integer.MAX_VALUE</code> elements, returns <code>Integer.MAX_VALUE</code>.
     *
     * @return total entry count
     */
    int size();

    /**
     * Closes the cache. Clears the internal content and releases any resource.
     *
     * @see javax.cache.CacheManager#destroyCache(String)
     */
    void destroy();

    /**
     * Determines whether this Cache instance has been destroyed.
     *
     * @return <code>true</code> if this Cache instance is destroyed; <code>false</code> if it is still open.
     */
    boolean isDestroyed();

    /**
     * Directly access local Cache Statistics.
     *
     * @return CacheStatistics instance or an empty statistics if not enabled.
     */
    CacheStatistics getLocalCacheStatistics();

    /**
     * Adds a CachePartitionLostListener.
     * <p>
     * The addPartitionLostListener returns a registration ID. This ID is needed to remove the
     * CachePartitionLostListener using the
     * {@link #removePartitionLostListener(UUID)} method.
     * <p>
     * There is no check for duplicate registrations, so if you register the listener twice, it will get events twice.
     * IMPORTANT: Please @see com.hazelcast.partition.PartitionLostListener for weaknesses.
     * IMPORTANT: Listeners registered from HazelcastClient may miss some of the cache partition lost events due
     * to design limitations.
     *
     * @param listener the added CachePartitionLostListener.
     * @return returns the registration ID for the CachePartitionLostListener.
     * @throws java.lang.NullPointerException if listener is null.
     * @see #removePartitionLostListener(UUID)
     */
    UUID addPartitionLostListener(CachePartitionLostListener listener);

    /**
     * Removes the specified cache partition lost listener.
     * Returns silently if there is no such listener added before.
     *
     * @param id ID of registered listener.
     * @return true if registration is removed, false otherwise.
     * @throws java.lang.NullPointerException if the given ID is null.
     */
    boolean removePartitionLostListener(UUID id);

    /**
     * <p>
     * Creates and returns a cluster wide iterator
     * to iterate on all entries owned by this cache.
     * </p>
     * <p>
     * The ordering of iteration over entries is undefined.
     * </p>
     * <p>
     * During iteration, any entries that are:
     * <ul>
     * <li>read will have their appropriate CacheEntryReadListeners notified.</li>
     * <li>removed will have their appropriate CacheEntryRemoveListeners notified.</li>
     * </ul>
     * {@link java.util.Iterator#next()} may return null if the entry is no
     * longer present, has expired or has been evicted.
     *
     * @param fetchSize size for fetching keys in bulk.
     *                  This size can be thought of as page size for iteration.
     *                  But notice that at every fetch, only keys are retrieved, not values.
     *                  Values are retrieved on each iterate.
     * @throws IllegalStateException if the cache is {@link #isClosed()}
     * @see #iterator()
     */
    Iterator<Cache.Entry<K, V>> iterator(int fetchSize);

}
