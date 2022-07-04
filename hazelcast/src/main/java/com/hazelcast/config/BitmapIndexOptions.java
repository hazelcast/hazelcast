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

package com.hazelcast.config;

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.QueryConstants;

import javax.annotation.Nonnull;
import java.io.IOException;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Configures indexing options specific to bitmap indexes.
 */
public class BitmapIndexOptions implements IdentifiedDataSerializable {

    /**
     * The default for {@link #getUniqueKey() unique key}.
     */
    public static final String DEFAULT_UNIQUE_KEY = QueryConstants.KEY_ATTRIBUTE_NAME.value();

    /**
     * The default for {@link #getUniqueKeyTransformation() unique key
     * transformation}.
     */
    public static final UniqueKeyTransformation DEFAULT_UNIQUE_KEY_TRANSFORMATION = UniqueKeyTransformation.OBJECT;

    /**
     * Defines an assortment of transformations which can be applied to {@link
     * BitmapIndexOptions#getUniqueKey() unique key} values.
     */
    public enum UniqueKeyTransformation {

        /**
         * Extracted unique key value is interpreted as an object value.
         * Non-negative unique ID is assigned to every distinct object value.
         */
        OBJECT("OBJECT", 0),

        /**
         * Extracted unique key value is interpreted as a whole integer value of
         * byte, short, int or long type. The extracted value is upcasted to
         * long (if necessary) and unique non-negative ID is assigned to every
         * distinct value.
         */
        LONG("LONG", 1),

        /**
         * Extracted unique key value is interpreted as a whole integer value of
         * byte, short, int or long type. The extracted value is upcasted to
         * long (if necessary) and the resulting value is used directly as an ID.
         */
        RAW("RAW", 2);

        private final String name;
        private final int id;

        UniqueKeyTransformation(String name, int id) {
            this.name = name;
            this.id = id;
        }

        /**
         * Resolves one of the {@link UniqueKeyTransformation} values by its name.
         *
         * @param name the name of the {@link UniqueKeyTransformation} value to
         *             resolve.
         * @return the resolved {@link UniqueKeyTransformation} value.
         * @throws IllegalArgumentException if the given name doesn't correspond
         *                                  to any known {@link
         *                                  UniqueKeyTransformation} name.
         */
        public static UniqueKeyTransformation fromName(String name) {
            if (StringUtil.isNullOrEmpty(name)) {
                throw new IllegalArgumentException("empty unique key transformation");
            }

            String upperCasedText = StringUtil.upperCaseInternal(name);
            if (upperCasedText.equals(OBJECT.name)) {
                return OBJECT;
            }
            if (upperCasedText.equals(LONG.name)) {
                return LONG;
            }
            if (upperCasedText.equals(RAW.name)) {
                return RAW;
            }

            throw new IllegalArgumentException("unexpected unique key transformation: " + name);
        }

        public static UniqueKeyTransformation fromId(int id) {
            for (UniqueKeyTransformation transformation : UniqueKeyTransformation.values()) {
                if (transformation.id == id) {
                    return transformation;
                }
            }

            throw new IllegalArgumentException("unexpected unique key transformation id: " + id);
        }

        /**
         * @return the id of this transformation.
         */
        public int getId() {
            return id;
        }

        @Override
        public String toString() {
            return name;
        }

    }

    private String uniqueKey;
    private UniqueKeyTransformation uniqueKeyTransformation;

    /**
     * Constructs a new bitmap index options instance with all options set to
     * default values.
     */
    public BitmapIndexOptions() {
        this.uniqueKey = DEFAULT_UNIQUE_KEY;
        this.uniqueKeyTransformation = DEFAULT_UNIQUE_KEY_TRANSFORMATION;
    }

    /**
     * Constructs a new bitmap index options instance by copying the passed
     * bitmap index options.
     */
    public BitmapIndexOptions(BitmapIndexOptions bitmapIndexOptions) {
        this.uniqueKey = bitmapIndexOptions.uniqueKey;
        this.uniqueKeyTransformation = bitmapIndexOptions.uniqueKeyTransformation;
    }

    /**
     * @return {@code true} if this options instance is configured with default
     * values, {@code false} otherwise.
     */
    boolean areDefault() {
        return DEFAULT_UNIQUE_KEY.equals(uniqueKey) && DEFAULT_UNIQUE_KEY_TRANSFORMATION == uniqueKeyTransformation;
    }

    /**
     * Returns the unique key attribute configured in this index config.
     * Defaults to {@code __key}. The unique key attribute is used as a source
     * of values which uniquely identify each entry being inserted into an index.
     *
     * @return the configured unique key attribute.
     */
    public String getUniqueKey() {
        return uniqueKey;
    }

    /**
     * Sets unique key attribute in this index config.
     *
     * @param uniqueKey a unique key attribute to configure.
     */
    public BitmapIndexOptions setUniqueKey(@Nonnull String uniqueKey) {
        checkNotNull(uniqueKey, "unique key can't be null");
        this.uniqueKey = uniqueKey;
        return this;
    }

    /**
     * Returns the unique key transformation configured in this index. Defaults
     * to {@link UniqueKeyTransformation#OBJECT OBJECT}. The transformation is
     * applied to every value extracted from {@link #getUniqueKey() unique key
     * attribue}.
     *
     * @return the configured unique key transformation.
     */
    public UniqueKeyTransformation getUniqueKeyTransformation() {
        return uniqueKeyTransformation;
    }

    /**
     * Sets unique key transformation in this index config.
     *
     * @param uniqueKeyTransformation a unique key transformation to configure.
     */
    public BitmapIndexOptions setUniqueKeyTransformation(@Nonnull UniqueKeyTransformation uniqueKeyTransformation) {
        checkNotNull(uniqueKeyTransformation, "unique key transformation can't be null");
        this.uniqueKeyTransformation = uniqueKeyTransformation;
        return this;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(uniqueKey);
        out.writeInt(uniqueKeyTransformation.getId());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        uniqueKey = in.readString();
        uniqueKeyTransformation = UniqueKeyTransformation.fromId(in.readInt());
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.BITMAP_INDEX_OPTIONS;
    }

    @Override
    public String toString() {
        return "BitmapIndexOptions{uniqueKey=" + uniqueKey + ", uniqueKeyTransformation=" + uniqueKeyTransformation + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BitmapIndexOptions that = (BitmapIndexOptions) o;

        if (!uniqueKey.equals(that.uniqueKey)) {
            return false;
        }

        return uniqueKeyTransformation == that.uniqueKeyTransformation;
    }

    @Override
    public int hashCode() {
        int result = uniqueKey.hashCode();
        result = 31 * result + uniqueKeyTransformation.hashCode();
        return result;
    }

}
