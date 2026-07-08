/*
 * Copyright 2026 Hazelcast Inc.
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
package com.hazelcast.jet.cdc;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;

public class TransactionStronglyTypedSerializer
        implements CompactSerializer<TransactionStronglyTyped> {

    @Nonnull
    @Override
    public TransactionStronglyTyped read(@Nonnull CompactReader reader) {
        return TransactionStronglyTyped.from(
                reader.readInt32("tokenId"),
                reader.readInt64("txnDatetime"),
                reader.readInt64("datetimeWithPrecision"),
                reader.readInt64("processingDate"),
                reader.readString("processingLocalDate"),
                reader.readInt64("time"),
                reader.readInt64("createDatetime")
                                            );
    }

    @Override
    public void write(@Nonnull CompactWriter writer, @Nonnull TransactionStronglyTyped o) {
        writer.writeInt32("tokenId", o.tokenId);
        writer.writeInt64("txnDatetime", o.txnDatetime.getTime());
        writer.writeInt64("datetimeWithPrecision", o.datetimeWithPrecision.toEpochMilli());
        writer.writeInt64("processingDate", o.processingDate.getTime());
        writer.writeString("processingLocalDate", o.processingLocalDate.toString());
        writer.writeInt64("time", o.time.toMillis());
        writer.writeInt64("createDatetime", TimeUnit.SECONDS.toNanos(o.createDatetime.getEpochSecond())
                + o.createDatetime.getNano());
    }

    @Nonnull
    @Override
    public String getTypeName() {
        return "TransactionStronglyTyped";
    }

    @Nonnull
    @Override
    public Class<TransactionStronglyTyped> getCompactClass() {
        return TransactionStronglyTyped.class;
    }
}
