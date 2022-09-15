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

package com.hazelcast.jet;

import com.hazelcast.cache.CacheEventType;
import com.hazelcast.cache.EventJournalCacheEvent;
import com.hazelcast.cache.impl.journal.CacheEventJournalFunctions;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.EventJournalMapEvent;
import com.hazelcast.map.impl.journal.MapEventJournalFunctions;

import javax.annotation.Nonnull;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.regex.Pattern;

import static com.hazelcast.jet.impl.util.ImdgUtil.wrapImdgFunction;
import static com.hazelcast.jet.impl.util.ImdgUtil.wrapImdgPredicate;

/**
 * Miscellaneous utility methods useful in DAG building logic.
 *
 * @since Jet 3.0
 */
public final class Util {

    private static final char[] ID_TEMPLATE = "0000-0000-0000-0000".toCharArray();
    private static final Pattern ID_PATTERN = Pattern.compile("(\\p{XDigit}{4}-){3}\\p{XDigit}{4}");

    private Util() {
    }

    /**
     * Returns a {@code Map.Entry} with the given key and value.
     */
    public static <K, V> Entry<K, V> entry(K k, V v) {
        return new SimpleImmutableEntry<>(k, v);
    }

    /**
     * Returns a predicate for {@link Sources#mapJournal} and
     * {@link Sources#remoteMapJournal} that passes only
     * {@link EntryEventType#ADDED ADDED} and {@link EntryEventType#UPDATED
     * UPDATED} events.
     */
    public static <K, V> PredicateEx<EventJournalMapEvent<K, V>> mapPutEvents() {
        return wrapImdgPredicate(MapEventJournalFunctions.mapPutEvents());
    }

    /**
     * Returns a predicate for {@link Sources#cacheJournal} and
     * {@link Sources#remoteCacheJournal} that passes only
     * {@link CacheEventType#CREATED CREATED} and {@link CacheEventType#UPDATED
     * UPDATED} events.
     */
    public static <K, V> PredicateEx<EventJournalCacheEvent<K, V>> cachePutEvents() {
        return wrapImdgPredicate(CacheEventJournalFunctions.cachePutEvents());
    }

    /**
     * Returns a projection that converts the {@link EventJournalMapEvent} to a
     * {@link java.util.Map.Entry} using the event's new value as a value.
     *
     * @see Sources#mapJournal
     * @see Sources#remoteMapJournal
     */
    public static <K, V> FunctionEx<EventJournalMapEvent<K, V>, Entry<K, V>> mapEventToEntry() {
        return wrapImdgFunction(MapEventJournalFunctions.mapEventToEntry());
    }

    /**
     * Returns a projection that extracts the new value from an {@link
     * EventJournalMapEvent}.
     *
     * @see Sources#mapJournal
     * @see Sources#remoteMapJournal
     */
    public static <K, V> FunctionEx<EventJournalMapEvent<K, V>, V> mapEventNewValue() {
        return wrapImdgFunction(MapEventJournalFunctions.mapEventNewValue());
    }

    /**
     * Returns a projection that converts the {@link EventJournalCacheEvent} to a
     * {@link java.util.Map.Entry} using the event's new value as a value.
     *
     * @see Sources#cacheJournal
     * @see Sources#remoteCacheJournal
     */
    public static <K, V> FunctionEx<EventJournalCacheEvent<K, V>, Entry<K, V>> cacheEventToEntry() {
        return wrapImdgFunction(CacheEventJournalFunctions.cacheEventToEntry());
    }

    /**
     * Returns a projection that extracts the new value from an {@link
     * EventJournalCacheEvent}.
     *
     * @see Sources#mapJournal
     * @see Sources#remoteMapJournal
     */
    public static <K, V> FunctionEx<EventJournalCacheEvent<K, V>, V> cacheEventNewValue() {
        return wrapImdgFunction(CacheEventJournalFunctions.cacheEventNewValue());
    }

    /**
     * Converts a {@code long} job or execution ID to a string representation.
     * Currently it is an unsigned 16-digit hex number.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    @Nonnull
    public static String idToString(long id) {
        char[] buf = Arrays.copyOf(ID_TEMPLATE, ID_TEMPLATE.length);
        String hexStr = Long.toHexString(id);
        for (int i = hexStr.length() - 1, j = 18; i >= 0; i--, j--) {
            buf[j] = hexStr.charAt(i);
            if (j == 15 || j == 10 || j == 5) {
                j--;
            }
        }
        return new String(buf);
    }

    /**
     * Parses the jobId formatted with {@link Util#idToString(long)}.
     * <p>
     * The method is lenient: if the string doesn't match the structure of the
     * ID generated by {@code idToString} or if the string is null, it will
     * return -1.
     *
     * @return the parsed ID or -1 if parsing failed.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public static long idFromString(String str) {
        if (str == null || !ID_PATTERN.matcher(str).matches()) {
            return -1;
        }
        str = StringUtil.removeCharacter(str, '-');
        return Long.parseUnsignedLong(str, 16);
    }

    /**
     * Takes the pathname of a classpath resource and returns a {@link Path}
     * that corresponds to it. This works only for resources that represent
     * files or directories on the local file system (i.e., the class loader
     * returns a {@code file:} URL).
     * <p>
     * For example, if you have a project directory {@code
     * src/main/resources/python}, the files in it will become classpath
     * resources with the {@code "python"} path prefix. Calling {@code
     * getFilePathOfClasspathResource("python")} will return the full path to
     * the above directory in the project.
     *
     * @param resourcePath the pathname of the classpath resource
     * @return a {@code Path} pointing to the project directory corresponding to the
     *         pathname
     * @since Jet 4.0
     */
    public static Path getFilePathOfClasspathResource(String resourcePath) {
        ClassLoader cl = Util.class.getClassLoader();
        URL url = cl.getResource(resourcePath);
        if (url == null) {
            throw new IllegalArgumentException(
                    "Resource doesn't exist or can't be represented by a URL: " + resourcePath);
        }
        if (!Objects.equals(url.getProtocol(), "file")) {
            throw new IllegalArgumentException(
                    "Resource is not on the file system, the URL protocol is " + url.getProtocol());
        }
        return Paths.get(url.getPath());
    }
}
