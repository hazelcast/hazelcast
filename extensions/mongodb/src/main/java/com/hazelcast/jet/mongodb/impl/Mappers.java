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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * Defines mappers used by MongoDB connector.
 * This includes {@linkplain MongoClientSettings#getDefaultCodecRegistry}
 * with {@link PojoCodecProvider} added for out-of-the-box POJO support.
 */
public final class Mappers {

    private static final Mappers INSTANCE = new Mappers();
    private final CodecRegistry pojoCodecRegistry;
    private final DecoderContext decoderContext;
    private final EncoderContext encoderContext;

    private Mappers() {
        CodecProvider pojoCodecProvider = PojoCodecProvider.builder().automatic(true).build();
        this.pojoCodecRegistry = fromRegistries(getDefaultCodecRegistry(), fromProviders(pojoCodecProvider));
        this.decoderContext = DecoderContext.builder().build();
        this.encoderContext = EncoderContext.builder().build();
    }

    public static <T> T map(Document doc, Class<T> type) {
        return INSTANCE.pojoCodecRegistry.get(type).decode(
                new BsonDocumentReader(doc.toBsonDocument()), INSTANCE.decoderContext);
    }

    /**
     * Returns default codec registry used by the connector, which includes
     * {@linkplain MongoClientSettings#getDefaultCodecRegistry} and {@link PojoCodecProvider}
     * for out-of-the-box POJO support.
     */
    @Nonnull
    public static CodecRegistry defaultCodecRegistry() {
        return INSTANCE.pojoCodecRegistry;
    }

    @Nonnull
    public static <T> FunctionEx<Document, T> toClass(Class<T> type) {
        return doc -> map(doc, type);
    }

    @Nonnull
    public static <T> BiFunctionEx<ChangeStreamDocument<Document>, Long, T> streamToClass(Class<T> type) {
        return (doc, ts) -> {
            assert doc.getFullDocument() != null;
            return map(doc.getFullDocument(), type);
        };
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    static <I> BsonDocument toBsonDocument(I item) {
        BsonDocument document = new BsonDocument();
        Class<I> aClass = (Class<I>) item.getClass();
        INSTANCE.pojoCodecRegistry.get(aClass).encode(new BsonDocumentWriter(document), item, INSTANCE.encoderContext);
        return document;
    }

    /**
     * Converts given {@link BsonDocument} to {@link Document}.
     */
    @Nonnull
    public static Document bsonDocumentToDocument(@Nonnull BsonDocument bsonDocument) {
        DocumentCodec codec = new DocumentCodec();
        DecoderContext decoderContext = DecoderContext.builder().build();
        return codec.decode(new BsonDocumentReader(bsonDocument), decoderContext);
    }

    /**
     * Converts given {@link Bson} to {@link Document}.
     */
    @Nonnull
    public static Document bsonToDocument(@Nonnull Bson bson) {
        if (bson instanceof Document) {
            return (Document) bson;
        }
        BsonDocument document = bson.toBsonDocument(BsonDocument.class, INSTANCE.pojoCodecRegistry);
        return bsonDocumentToDocument(document);
    }
}
