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

package com.hazelcast.core;

import com.hazelcast.nio.serialization.DataSerializable;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Cluster member interface. The default implementation
 * {@link com.hazelcast.instance.MemberImpl} violates the Java Serialization contract.
 * It should be serialized/deserialized by Hazelcast.
 *
 * @see Cluster
 * @see MembershipListener
 */
public interface Member extends DataSerializable, Endpoint {

    /**
     * Returns true if this member is the local member.
     *
     * @return <tt>true</tt> if this member is the
     *         local member, <tt>false</tt> otherwise.
     */
    boolean localMember();

    /**
     * Returns the InetSocketAddress of this member.
     *
     * @return InetSocketAddress of this member
     *
     * @deprecated use {@link #getSocketAddress()} instead
     */
    @Deprecated
    InetSocketAddress getInetSocketAddress();

    /**
     * Returns the socket address of this member.
     *
     * @return the socket address of this member
     */
    InetSocketAddress getSocketAddress();

    /**
     * Returns the UUID of this member.
     *
     * @return the UUID of this member.
     */
    String getUuid();

    /**
     * Returns configured attributes for this member.<br/>
     * <b>This method might not be available on all native clients.</b>
     *
     * @return configured attributes for this member.
     * @since 3.2
     */
    Map<String, Object> getAttributes();

    /**
     * Returns the value of the specified key for this member or
     * null if value is undefined.
     *
     * @param key The key to lookup.
     * @return The value for this member key.
     * @since 3.2
     */
    String getStringAttribute(String key);

    /**
     * Defines a key-value pair string attribute for this member available
     * to other cluster members.
     *
     * @param key The key for this property.
     * @param value The value that corresponds to this attribute and this member.
     * @since 3.2
     */
    void setStringAttribute(String key, String value);

    /**
     * Returns the value of the specified key for this member, or
     * null if the value is undefined.
     *
     * @param key The key to look up
     * @return the value for this member key
     * @since 3.2
     */
    Boolean getBooleanAttribute(String key);

    /**
     * Defines a key-value pair boolean attribute for this member that is available
     * to other cluster members.
     *
     * @param key The key for this property.
     * @param value The value that corresponds to this attribute and this member.
     * @since 3.2
     */
    void setBooleanAttribute(String key, boolean value);

    /**
     * Returns the value of the specified key for this member or
     * null if the value is undefined.
     *
     * @param key The key to look up.
     * @return The value for this member key.
     * @since 3.2
     */
    Byte getByteAttribute(String key);

    /**
     * Defines a key-value pair byte attribute for this member available
     * to other cluster members.
     *
     * @param key The key for this property.
     * @param value The value that corresponds to this attribute and this member.
     * @since 3.2
     */
    void setByteAttribute(String key, byte value);

    /**
     * Returns the value of the specified key for this member or
     * null if value is undefined.
     *
     * @param key The key to look up.
     * @return The value for this member key.
     * @since 3.2
     */
    Short getShortAttribute(String key);

    /**
     * Defines a key-value pair short attribute for this member available
     * to other cluster members.
     *
     * @param key The key for this property.
     * @param value The value that corresponds to this attribute and this member.
     * @since 3.2
     */
    void setShortAttribute(String key, short value);

    /**
     * Returns the value of the specified key for this member or
     * null if value is undefined.
     *
     * @param key The key to look up.
     * @return The value for this members key.
     * @since 3.2
     */
    Integer getIntAttribute(String key);

    /**
     * Defines a key-value pair int attribute for this member available
     * to other cluster members.
     *
     * @param key The key for this property.
     * @param value The value that corresponds to this attribute and this member.
     * @since 3.2
     */
    void setIntAttribute(String key, int value);

    /**
     * Returns the value of the specified key for this member or
     * null if value is undefined.
     *
     * @param key The key to look up.
     * @return The value for this members key.
     * @since 3.2
     */
    Long getLongAttribute(String key);

    /**
     * Defines a key-value pair long attribute for this member available
     * to other cluster members.
     *
     * @param key The key for this property.
     * @param value The value that corresponds to this attribute and this member.
     * @since 3.2
     */
    void setLongAttribute(String key, long value);

    /**
     * Returns the value of the specified key for this member or
     * null if value is undefined.
     *
     * @param key The key to look up.
     * @return The value for this member key.
     * @since 3.2
     */
    Float getFloatAttribute(String key);

    /**
     * Defines a key-value pair float attribute for this member available
     * to other cluster members.
     *
     * @param key The key for this property.
     * @param value The value that corresponds to this attribute and this member.
     * @since 3.2
     */
    void setFloatAttribute(String key, float value);

    /**
     * Returns the value of the specified key for this member or
     * null if value is undefined.
     *
     * @param key The key to look up.
     * @return The value for this members key.
     * @since 3.2
     */
    Double getDoubleAttribute(String key);

    /**
     * Defines a key-value pair double attribute for this member available
     * to other cluster members.
     *
     * @param key The key for this property.
     * @param value The value that corresponds to this attribute and this member.
     * @since 3.2
     */
    void setDoubleAttribute(String key, double value);

    /**
     * Removes a key-value pair attribute for this member if given key was
     * previously assigned as an attribute.<br/>
     * If key wasn't assigned to a value this method does nothing.
     *
     * @param key The key to be deleted from the member attributes
     * @since 3.2
     */
    void removeAttribute(String key);

}
