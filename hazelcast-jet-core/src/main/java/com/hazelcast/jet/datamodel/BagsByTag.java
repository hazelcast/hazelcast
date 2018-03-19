/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Objects;
import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * A heterogeneous map from {@code Tag<E>} to {@code Collection<E>}, where
 * {@code E} can be different for each tag. Useful as a container of
 * co-grouped items, where each tag corresponds to one contributing
 * pipeline pipeline.
 * <p>
 * This is a less type-safe, but more flexible alternative to the {@link
 * TwoBags} and {@link ThreeBags} containers, which have a fixed (and
 * limited) number of integer-indexed, statically-typed fields. {@code
 * BagsByTag} has a variable number of tag-indexed fields whose whose
 * static type is encoded in the tags.
 */
public class BagsByTag {
    private final Map<Tag<?>, Collection> components = new HashMap<>();

    /**
     * Accepts an argument list of alternating tags and collections, interprets
     * them as a list of tag-bag pairs, and returns a {@code BagsByTag}
     * populated with these pairs. Doesn't retain the supplied collections, but
     * copies them into newly created ones.
     */
    @Nonnull
    public static BagsByTag bagsByTag(@Nonnull Object... tagsAndBags) {
        BagsByTag bbt = new BagsByTag();
        for (int i = 0; i < tagsAndBags.length;) {
            bbt.components.put((Tag<?>) tagsAndBags[i++], new ArrayList<>((Collection<?>) tagsAndBags[i++]));
        }
        return bbt;
    }

    /**
     * Retrieves the bag associated with the supplied tag. Throws
     * {@code IllegalArgumentException} if there is none.
     *
     * @param tag the lookup tag
     * @param <E> the type of items in the returned bag
     * @return the associated bag
     * @throws IllegalArgumentException if there is no bag associated with the tag
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public <E> Collection<E> bag(@Nonnull Tag<E> tag) {
        Collection<E> bag = components.get(tag);
        if (bag == null) {
            throw new IllegalArgumentException("No bag associated with tag " + tag);
        }
        return bag;
    }

    /**
     * Ensures that there is a mapping from the supplied tag to a bag,
     * creating an empty one if necessary. Returns the bag.
     *
     * @param tag the lookup tag
     * @param <E> the type of items in the returned bag
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public <E> Collection<E> ensureBag(@Nonnull Tag<E> tag) {
        return (Collection<E>) components.computeIfAbsent(tag, x -> new ArrayList<>());
    }

    /**
     * Merges the contents of the supplied bag container into this one. If both
     * containers have a mapping for a given tag, appends the supplied
     * container's items to the bag in this container; otherwise establishes a
     * new mapping in this container to a copy of the supplied container's bag.
     * <p>
     * Does not modify the supplied container.
     *
     * @param that the container to combine with this one.
     */
    @SuppressWarnings("unchecked")
    public void combineWith(@Nonnull BagsByTag that) {
        that.components.forEach((k, v) -> ensureBag(k).addAll(v));
    }

    /**
     * Returns a safe copy of this container.
     */
    public BagsByTag finish() {
        BagsByTag copy = new BagsByTag();
        copy.components.putAll(components);
        return copy;
    }

    @Override
    public boolean equals(Object o) {
        return this == o
                || o instanceof BagsByTag
                && Objects.equals(this.components, ((BagsByTag) o).components);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(components);
    }

    @Override
    public String toString() {
        return "BagsByTag " + components.toString();
    }


    // These two methods are used by the Hazelcast serializer hook

    @Nonnull
    Set<Entry<Tag<?>, Collection>> entrySet() {
        return components.entrySet();
    }

    void put(@Nonnull Tag<?> tag, @Nonnull ArrayList list) {
        components.put(tag, list);
    }
}
