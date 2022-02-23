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

import com.hazelcast.internal.serialization.SerializableByConvention;

import java.io.Serializable;

/**
 * A tag object useful as a key in heterogeneous maps. Carries static type
 * information in its type parameter, which is expected to correspond to
 * the type of the item retrieved from the map.
 * <p>
 * Tags are also used by hash-join and co-group builder objects. The same
 * tag supplied to the builder is used to retrieve the data from the
 * heterogeneous maps ({@link ItemsByTag}) that appear in the output.
 *
 * @param <T> the type of the data associated with the tag
 *
 * @since Jet 3.0
 */
@SerializableByConvention
public final class Tag<T> implements Comparable<Tag<?>>, Serializable {

    private static final long serialVersionUID = 1L;

    private static final Tag TAG_0 = new Tag(0);
    private static final Tag TAG_1 = new Tag(1);
    private static final Tag TAG_2 = new Tag(2);

    private final int index;

    private Tag(int index) {
        this.index = index;
    }

    /**
     * Returns a tag object associated with the specified index. The
     * tag's type parameter is inferred from the call site. The method
     * will not necessarily create a new tag object.
     */
    public static <T> Tag<T> tag(int index) {
        return index == 0 ? TAG_0
                : index == 1 ? TAG_1
                : index == 2 ? TAG_2
                : new Tag<>(index);
    }

    /**
     * Returns the index associated with this tag. It can refer to the
     * index of a contributing stream in a hash-join or co-group operation,
     * or to the index used internally to store the data associated with
     * the tag.
     */
    public int index() {
        return index;
    }

    /**
     * Returns the tag constant {@link #TAG_0}.
     */
    @SuppressWarnings("unchecked")
    public static <T> Tag<T> tag0() {
        return TAG_0;
    }

    /**
     * Returns the tag constant {@link #TAG_1}.
     */
    @SuppressWarnings("unchecked")
    public static <T> Tag<T> tag1() {
        return TAG_1;
    }

    /**
     * Returns the tag constant {@link #TAG_2}.
     */
    @SuppressWarnings("unchecked")
    public static <T> Tag<T> tag2() {
        return TAG_2;
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj ||
                obj instanceof Tag && this.index == ((Tag) obj).index;
    }

    @Override
    public int hashCode() {
        return index;
    }

    @Override
    public int compareTo(Tag<?> that) {
        return Integer.compare(this.index, that.index);
    }

    @Override
    public String toString() {
        return "Tag" + index;
    }
}
