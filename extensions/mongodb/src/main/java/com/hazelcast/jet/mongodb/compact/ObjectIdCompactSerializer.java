package com.hazelcast.jet.mongodb.compact;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import org.bson.types.ObjectId;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Compact serializer for {@link ObjectId}.
 */
public class ObjectIdCompactSerializer implements CompactSerializer<ObjectId> {
    @Nonnull
    @Override
    public ObjectId read(@Nonnull CompactReader reader) {
        return new ObjectId(requireNonNull(reader.readString("hex")));
    }

    @Override
    public void write(@Nonnull CompactWriter writer, @Nonnull ObjectId object) {
        writer.writeString("hex", object.toHexString());
    }

    @Nonnull
    @Override
    public String getTypeName() {
        return "ObjectId";
    }

    @Nonnull
    @Override
    public Class<ObjectId> getCompactClass() {
        return ObjectId.class;
    }
}
