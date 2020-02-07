/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.EventJournalMapEvent;
import com.hazelcast.map.impl.journal.MapEventJournalFunctions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Queue;
import java.util.regex.Pattern;

import static com.hazelcast.jet.impl.util.ImdgUtil.wrapImdgFunction;
import static com.hazelcast.jet.impl.util.ImdgUtil.wrapImdgPredicate;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;

/**
 * Miscellaneous utility methods useful in DAG building logic.
 *
 * @since 3.0
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
     * Parses the jobId formatted with {@link
     * Util#idToString(long)}.
     * <p>
     * The method is lenient: if the string doesn't match the structure
     * output by {@code idToString} or if the string is null, it will return
     * -1.
     *
     * @return the parsed ID or -1 if parsing failed.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public static long idFromString(String str) {
        if (str == null || !ID_PATTERN.matcher(str).matches()) {
            return -1;
        }
        str = str.replaceAll("-", "");
        return Long.parseUnsignedLong(str, 16);
    }

    /**
     * Takes a classpath "directory" (a path prefix) and exports it to a
     * temporary filesystem directory. Returns the created temporary directory
     * path.
     * <p>
     * For example, if you have a project directory {@code
     * src/main/resources/python}, the files in it will become classpath
     * resources with the {@code "python"} path prefix. Calling {@code
     * materializeClasspathDirectory("python")} will return the path to
     * a temp directory with all the files inside it.
     * <p>
     * The given classpath directory may contain nested directories. Since the
     * classpath resources don't have the notion of a "directory", this method
     * uses a heuristic to determine whether a given resource name represents a
     * directory. First, it assumes it is not a directory if the filename part
     * contains a dot. Second, it opens the resource as a text file and checks
     * each line whether it's a string denoting an existing classpath resource.
     * If all lines pass the check, then it's deemed to be a directory.
     *
     * @param classpathPrefix the path prefix of the classpath resources to materialize
     * @return a {@code Path} pointing to the created temporary directory
     * @since 4.0
     */
    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", justification =
                        "False positive on try-with-resources as of JDK11")
    public static Path copyClasspathDirectory(String classpathPrefix) throws IOException {
        Path destPathBase = Files.createTempDirectory("hazelcast-jet-");
        ClassLoader cl = Util.class.getClassLoader();
        Queue<String> dirNames = new ArrayDeque<>(singletonList(""));
        for (String dirName; (dirName = dirNames.poll()) != null; ) {
            String dirResourceName = classpathPrefix + (dirName.isEmpty() ? "" : '/' + dirName);
            try (InputStream listingStream = Objects.requireNonNull(cl.getResourceAsStream(dirResourceName));
                 BufferedReader rdr = new BufferedReader(new InputStreamReader(listingStream, UTF_8))
            ) {
                Iterable<String> relativeNames = () -> rdr.lines().iterator();
                for (String relName : relativeNames) {
                    String subResourceName = dirResourceName + '/' + relName;
                    String nameRelativeToBase = dirName + (dirName.isEmpty() ? "" : "/") + relName;
                    Path destPath = destPathBase.resolve(nameRelativeToBase);
                    if (isDirectory(subResourceName, cl)) {
                        Files.createDirectories(destPath);
                        dirNames.add(nameRelativeToBase);
                        continue;
                    }
                    try (InputStream in = cl.getResourceAsStream(subResourceName)) {
                        Files.copy(in, destPath);
                    }
                }
            }
        }
        return destPathBase;
    }

    private static boolean isDirectory(String resourceName, ClassLoader cl) {
        if (resourceName.indexOf('.') >= 0) {
            return false;
        }
        try (InputStream listingStream = Objects.requireNonNull(cl.getResourceAsStream(resourceName));
             BufferedReader rdr = new BufferedReader(new InputStreamReader(listingStream, UTF_8))
        ) {
            return rdr.lines().allMatch(filename -> cl.getResource(resourceName + '/' + filename) != null);
        } catch (IOException e) {
            return false;
        }
    }
}
