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

package com.hazelcast.nio.serialization;

/**
 * This interface can be implemented by objects returned by a deserializer for making them accessible for querying and
 * indexing by a server that doesn't actually have the bytecode of the classes of the objects that are stored in the cache.
 * This is useful in in a client/server scenario.<br />
 * An example of this would be if your hazelcast clients serialize objects into JSON using a custom serializer. The
 * result would be something like this (for a customer record):
 * <pre>
 *     {
 *         "name": "John Doe",
 *         "address": {
 *             "street":"10, Sunset blvd."
 *             "city":"Los Angeles"
 *         }
 *     }
 * </pre>
 * The client serialization config for the customer type is pretty straightforward:
 * <pre>
 *     clientSerializationConfig.addSerializerConfig(new SerializerConfig()
 *      .setTypeClass(Customer.class)
 *      .setImplementation(new CustomerJsonSerializer(CUSTOMER_TYPE_ID)));
 * </pre>
 * But the server can not use this serializer since it does not know about the Customer class. The problem is that
 * without a serializer it can not deserialize the objects in cache and can thus not index or query them. the solution
 * is to implement a generic JsonSerializer that returns objects of a type that implements AttributeAccessible. This
 * class just gets the attribute using e.g. the JSON library.
 * <pre>
 *     serverSerializationConfig.addSerializerConfig(new SerializerConfig()
 *      .setTypeClass(JsonAttributeAccessible.class)
 *      .setImplementation(new JsonSerializer(CUSTOMER_TYPE_ID)));
 * </pre>
 * Now we can create an index on a field and the server is able to generate this index using the generic serializer:
 * <pre>
 *     customerMap.addIndex("address.street", false);
 * </pre>
 * We can also query fields with/without an index:
 * <pre>
 *     EntryObject e = new PredicateBuilder().getEntryObject();
 *     laCustomers = customerMap.values(e.get("address.city").equal("Los Angeles"));
 * </pre>
 */
public interface AttributeAccessible {
    /**
     * Reads the value of the specified attribute. If the method you use for deserialization allows partial
     * deserialization it might be a good idea to use this to improve performance. Since only the value of one attribute
     * is queried we do not need to deserialize the whole object if we can avoid it.
     *
     * @param attributeName Name of the attribute as specified in the addIndex or query get(). Is usually dot delimited,
     *                      e.g. address.city . Is never null.
     * @return Value of the attribute. May only be primitive type as listed in ReflectionHelper#getAttributeType. May be
     * null if the specified attribute does not exist on this object.
     */
    Object getValue(String attributeName);

    /**
     * Reads the type of the specified attribute.
     *
     * @param attributeName Name of the attribute as specified in the addIndex or query get(). Is usually dot delimited,
     *                      e.g. address.city . Is never null.
     * @return Type of the attribute. Multiple calls to this method with the same attribute name are
     * required to return the same type. If the specified attribute does not exist on the type of this object then
     * null may be returned. But this means in turn that an index on this non-existent field may never have been created
     * in the first place
     */
    Class<?> getType(String attributeName);
}
