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

public class SqlPageCodec {

    private SqlPageCodec() {
    }

    @SuppressWarnings("unchecked")
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
                    ListMultiFrameCodec.encode(clientMessage, (Iterable<Data>) column, DataCodec::encodeNullable);

                    break;
            }
        }

        clientMessage.add(END_FRAME.copy());
    }

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
