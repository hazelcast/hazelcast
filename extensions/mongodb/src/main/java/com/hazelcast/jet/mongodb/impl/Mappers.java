/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.mongodb.impl;

import com.hazelcast.function.FunctionEx;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonDocumentReader;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import javax.annotation.Nonnull;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public final class Mappers {

    private static final Mappers INSTANCE = new Mappers();
    private final CodecRegistry pojoCodecRegistry;
    private final DecoderContext decoderContext;

    private Mappers() {
        CodecProvider pojoCodecProvider = PojoCodecProvider.builder().automatic(true).build();
        this.pojoCodecRegistry = fromRegistries(getDefaultCodecRegistry(), fromProviders(pojoCodecProvider));
        this.decoderContext = DecoderContext.builder().build();
    }

    public static <T> T map(Document doc, Class<T> type) {
        return INSTANCE.pojoCodecRegistry.get(type).decode(
                new BsonDocumentReader(doc.toBsonDocument()), INSTANCE.decoderContext);
    }

    @Nonnull
    public static CodecRegistry defaultCodecRegistry() {
        return INSTANCE.pojoCodecRegistry;
    }

    @Nonnull
    public static <T> FunctionEx<Document, T> toClass(Class<T> type) {
        return doc -> map(doc, type);
    }

    @Nonnull
    public static <T> FunctionEx<ChangeStreamDocument<Document>, T> streamToClass(Class<T> type) {
        return doc -> {
            assert doc.getFullDocument() != null;
            return map(doc.getFullDocument(), type);
        };
    }

}
