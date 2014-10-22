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

package com.hazelcast.cache.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * <p>Hazelcast provides custom expiry policy on each cache operation. This class comes in handy for that
 * functionality.
 * <p>
 *     Sample usage:
 * <pre>
 *     <code>ICache&lt;String , SessionData&gt; icache =  cache.unwrap( ICache.class );
 *     HazelcastExpiryPolicy customExpiry = new HazelcastExpiryPolicy(20, 30, 40, TimeUnit.SECONDS);
 *     icache.put(&quot;key1&quot;, sessionData, customExpiry );
 *     </code>
 * </pre>
 * </p>
 */
//todo Isn't that public API?
public class HazelcastExpiryPolicy implements ExpiryPolicy, IdentifiedDataSerializable {

    private Duration create;
    private Duration access;
    private Duration update;

    /**
     * Constructs an expiry policy with provided values in milliseconds.
     * @param createMillis
     * @param accessMillis
     * @param updateMillis
     */
    public HazelcastExpiryPolicy(long createMillis, long accessMillis, long updateMillis) {
        this(new Duration(TimeUnit.MILLISECONDS, createMillis), new Duration(TimeUnit.MILLISECONDS, accessMillis),
                new Duration(TimeUnit.MILLISECONDS, updateMillis));
    }

    /**
     * Constructs an expiry policy with provided values and TimeUnit.
     * @param createDurationAmount
     * @param accessDurationAmount
     * @param updateDurationAmount
     * @param timeUnit
     */
    public HazelcastExpiryPolicy(long createDurationAmount, long accessDurationAmount, long updateDurationAmount,
                                 TimeUnit timeUnit) {
        this(new Duration(timeUnit, createDurationAmount), new Duration(timeUnit, accessDurationAmount),
                new Duration(timeUnit, updateDurationAmount));
    }

    /**
     * Expiry policy wrapper.
     * @param expiryPolicy
     */
    public HazelcastExpiryPolicy(ExpiryPolicy expiryPolicy) {
        if (expiryPolicy != null) {
            this.create = expiryPolicy.getExpiryForCreation();
            this.access = expiryPolicy.getExpiryForAccess();
            this.update = expiryPolicy.getExpiryForUpdate();
        }
    }

    /**
     * Constructs an expiry policy with provided values in Duration.
     * @param create
     * @param access
     * @param update
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
