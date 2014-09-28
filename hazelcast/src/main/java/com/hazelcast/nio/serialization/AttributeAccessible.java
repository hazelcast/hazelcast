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
     * is queried we do not need to deserialize th whole object if we can avoid it.
     *
     * @param attributeName Name of the attribute as specified in the addIndex or query get(). Is usually dot delimited,
     *                      e.g. address.city
     * @return Value of the attribute
     */
    Object getAttribute(String attributeName);
}
