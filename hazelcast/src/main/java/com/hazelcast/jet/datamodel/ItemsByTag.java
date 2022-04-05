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

package com.hazelcast.jet.datamodel;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

/**
 * A heterogeneous map from {@code Tag<E>} to {@code E}, where {@code E}
 * can be different for each tag. The value associated with a tag may be
 * {@code null}.
 * <p>
 * This is a less type-safe, but more flexible alternative to a tuple. The
 * tuple has a fixed number of integer-indexed, statically-typed fields,
 * and {@code ItemsByTag} has a variable number of tag-indexed fields whose
 * whose static type is encoded in the tags.
 *
 * @since Jet 3.0
 */
public final class ItemsByTag {
    private static final Object NONE = new Object();

    private final Map<Tag<?>, Object> map = new HashMap<>();

    /**
     * Accepts an argument list of alternating tags and values, interprets
     * them as a list of tag-value pairs, and returns an {@code ItemsByTag}
     * populated with these pairs.
     */
    @Nonnull
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static ItemsByTag itemsByTag(@Nonnull Object... tagsAndVals) {
        ItemsByTag ibt = new ItemsByTag();
        for (int i = 0; i < tagsAndVals.length;) {
            ibt.put((Tag) tagsAndVals[i++], tagsAndVals[i++]);
        }
        return ibt;
    }

    /**
     * Retrieves the value associated with the supplied tag and throws an
     * exception if there is none. The tag argument must not be {@code null},
     * but the returned value may be, if a {@code null} value is explicitly
     * associated with a tag.
     *
     * @throws IllegalArgumentException if there is no value associated with the supplied tag
     */
    @Nullable
    @SuppressWarnings("unchecked")
    public <E> E get(@Nonnull Tag<E> tag) {
        Object got = map.getOrDefault(tag, NONE);
        if (got == NONE) {
            throw new IllegalArgumentException("No value associated with " + tag);
        }
        return (E) got;
    }

    /**
     * Associates the supplied value with the supplied tag. The tag must not be
     * {@code null}, but the value may be, and in that case the tag will be
     * associated with a {@code null} value.
     */
    public <E> void put(@Nonnull Tag<E> tag, @Nullable E value) {
        map.put(tag, value);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ItemsByTag
                && Objects.equals(this.map, ((ItemsByTag) o).map);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(map);
    }

    @Override
    public String toString() {
        return "ItemsByTag" + map;
    }

    // For the Hazelcast serializer hook
    @Nonnull
    Set<Entry<Tag<?>, Object>> entrySet() {
        return map.entrySet();
    }
}
