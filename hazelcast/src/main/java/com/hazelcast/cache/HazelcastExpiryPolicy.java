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

package com.hazelcast.cache;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * <p>Hazelcast provides overloads of the typical cache operations with a custom
 * {@link javax.cache.expiry.ExpiryPolicy} parameter.<br>
 * This class provides a custom implementation of an {@link javax.cache.expiry.ExpiryPolicy} to
 * react on all three types of policies:
 * <ul>
 *     <li>Create</li>
 *     <li>Access</li>
 *     <li>Update</li>
 * </ul>
 * <p>
 *     Sample usage:
 * <pre>
 *   ICache&lt;Key, Value&gt; unwrappedCache =  cache.unwrap( ICache.class );
 *   HazelcastExpiryPolicy customExpiry = new HazelcastExpiryPolicy(20, 30, 40, TimeUnit.SECONDS);
 *   unwrappedCache.put(&quot;key1&quot;, value, customExpiry );
 * </pre>
 * </p>
 *
 * @since 3.3.1
 */
public class HazelcastExpiryPolicy implements ExpiryPolicy, IdentifiedDataSerializable, Serializable {

    private Duration create;
    private Duration access;
    private Duration update;

    /**
     * Constructs an expiry policy with provided values for creation, access and update in milliseconds.
     *
     * @param createMillis expiry time in milliseconds after creation
     * @param accessMillis expiry time in milliseconds after last access
     * @param updateMillis expiry time in milliseconds after last update
     */
    public HazelcastExpiryPolicy(long createMillis, long accessMillis, long updateMillis) {
        this(new Duration(TimeUnit.MILLISECONDS, createMillis), new Duration(TimeUnit.MILLISECONDS, accessMillis),
                new Duration(TimeUnit.MILLISECONDS, updateMillis));
    }

    /**
     * Constructs an expiry policy with provided values for creation, access and update as well as a
     * {@link java.util.concurrent.TimeUnit} to convert those values to internally used time unites.
     *
     * @param createDurationAmount expiry time after creation
     * @param accessDurationAmount expiry time after last access
     * @param updateDurationAmount expiry time after last update
     * @param timeUnit time unit of the previous value parameters
     */
    public HazelcastExpiryPolicy(long createDurationAmount, long accessDurationAmount, long updateDurationAmount,
                                 TimeUnit timeUnit) {
        this(new Duration(timeUnit, createDurationAmount), new Duration(timeUnit, accessDurationAmount),
                new Duration(timeUnit, updateDurationAmount));
    }

    /**
     * Copy Constructor for an already existing {@link javax.cache.expiry.ExpiryPolicy}. Values are
     * copied to the internal state as is.
     *
     * @param expiryPolicy expiry policy to copy
     */
    public HazelcastExpiryPolicy(ExpiryPolicy expiryPolicy) {
        if (expiryPolicy != null) {
            this.create = expiryPolicy.getExpiryForCreation();
            this.access = expiryPolicy.getExpiryForAccess();
            this.update = expiryPolicy.getExpiryForUpdate();
        }
    }

    /**
     * Constructs an expiry policy with provided values for creation, access and update by providing
     * instances of the {@link javax.cache.expiry.Duration} class.
     *
     * @param create expiry duration after creation
     * @param access expiry duration after last access
     * @param update expiry duration after last update
     */
    public HazelcastExpiryPolicy(Duration create, Duration access, Duration update) {
        this.create = create;
        this.access = access;
        this.update = update;
    }

    @Override
    public Duration getExpiryForCreation() {
        return create;
    }

    @Override
    public Duration getExpiryForAccess() {
        return access;
    }

    @Override
    public Duration getExpiryForUpdate() {
        return update;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.EXPIRY_POLICY;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        writeDuration(out, create);
        writeDuration(out, access);
        writeDuration(out, update);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        create = readDuration(in);
        access = readDuration(in);
        update = readDuration(in);
    }

    private void writeDuration(ObjectDataOutput out, Duration duration)
            throws IOException {
        if (duration != null) {
            out.writeLong(duration.getDurationAmount());
            out.writeInt(duration.getTimeUnit().ordinal());
        }
    }

    private Duration readDuration(ObjectDataInput in)
            throws IOException {
        long da = in.readLong();
        if (da > -1) {
            TimeUnit tu = TimeUnit.values()[in.readInt()];
            return new Duration(tu, da);
        }
        return null;
    }
}
