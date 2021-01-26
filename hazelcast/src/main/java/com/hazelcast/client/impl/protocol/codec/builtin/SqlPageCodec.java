/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.client.SqlPage;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;

public final class SqlPageCodec {

    private SqlPageCodec() {
    }

    @SuppressWarnings({"unchecked", "checkstyle:CyclomaticComplexity"})
    public static void encode(ClientMessage clientMessage, SqlPage sqlPage) {
        clientMessage.add(BEGIN_FRAME.copy());

        // Write the "last" flag.
        byte[] content = new byte[]{(byte) (sqlPage.isLast() ? 1 : 0)};
        clientMessage.add(new ClientMessage.Frame(content));

        // Write column types.
        List<SqlColumnType> columnTypes = sqlPage.getColumnTypes();
        List<Integer> columnTypeIds = new ArrayList<>(columnTypes.size());

        for (SqlColumnType columnType : columnTypes) {
            columnTypeIds.add(columnType.getId());
        }

        ListIntegerCodec.encode(clientMessage, columnTypeIds);

        // Write columns.
        for (int i = 0; i < sqlPage.getColumnCount(); i++) {
            SqlColumnType columnType = columnTypes.get(i);
            Iterable<?> column = sqlPage.getColumnValuesForServer(i);

            switch (columnType) {
                case VARCHAR:
                    ListMultiFrameCodec.encodeContainsNullable(clientMessage, (Iterable<String>) column, StringCodec::encode);

                    break;

                case BOOLEAN:
                    ListCNBooleanCodec.encode(clientMessage, (Iterable<Boolean>) column);

                    break;

                case TINYINT:
                    ListCNByteCodec.encode(clientMessage, (Iterable<Byte>) column);

                    break;

                case INTEGER:
                    ListCNIntegerCodec.encode(clientMessage, (Iterable<Integer>) column);

                    break;

                case BIGINT:
                    ListCNLongCodec.encode(clientMessage, (Iterable<Long>) column);

                    break;

                case SMALLINT:
                case DECIMAL:
                case REAL:
                case DOUBLE:
                case DATE:
                case TIME:
                case TIMESTAMP:
                case TIMESTAMP_WITH_TIME_ZONE:
                case NULL:
                case OBJECT:
                    // TODO: All types except for NULL and OBJECT should be serialized with a custom codecs before 4.2
                    assert SqlPage.convertToData(columnType);

                    ListMultiFrameCodec.encode(clientMessage, (Iterable<Data>) column, DataCodec::encodeNullable);

                    break;

                default:
                    throw new IllegalStateException("Unknown type " + columnType);
            }
        }

        clientMessage.add(END_FRAME.copy());
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public static SqlPage decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        // Read the "last" flag.
        boolean isLast = iterator.next().content[0] == 1;

        // Read column types.
        List<Integer> columnTypeIds = ListIntegerCodec.decode(iterator);
        List<SqlColumnType> columnTypes = new ArrayList<>(columnTypeIds.size());

        // Read columns.
        List<List<?>> columns = new ArrayList<>(columnTypeIds.size());

        for (int columnTypeId : columnTypeIds) {
            SqlColumnType columnType = SqlColumnType.getById(columnTypeId);
            assert columnType != null;

            columnTypes.add(columnType);

            switch (columnType) {
                case VARCHAR:
                    columns.add(ListMultiFrameCodec.decodeContainsNullable(iterator, StringCodec::decode));

                    break;

                case BOOLEAN:
                    columns.add(ListCNBooleanCodec.decode(iterator));

                    break;

                case TINYINT:
                    columns.add(ListCNByteCodec.decode(iterator));

                    break;

                case INTEGER:
                    columns.add(ListCNIntegerCodec.decode(iterator));

                    break;

                case BIGINT:
                    columns.add(ListCNLongCodec.decode(iterator));

                    break;

                case SMALLINT:
                case DECIMAL:
                case REAL:
                case DOUBLE:
                case DATE:
                case TIME:
                case TIMESTAMP:
                case TIMESTAMP_WITH_TIME_ZONE:
                case NULL:
                case OBJECT:
                    // TODO: All types except for NULL and OBJECT should be serialized with a custom codecs before 4.2
                    assert SqlPage.convertToData(columnType);

                    columns.add(ListMultiFrameCodec.decode(iterator, DataCodec::decodeNullable));

                    break;

                default:
                    throw new IllegalStateException("Unknown type " + columnType);
            }
        }

        fastForwardToEndFrame(iterator);

        return SqlPage.fromColumns(columnTypes, columns, isLast);
    }
}
