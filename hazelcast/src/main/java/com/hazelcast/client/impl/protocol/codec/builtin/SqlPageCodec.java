package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.client.SqlPage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;

public class SqlPageCodec {


    private SqlPageCodec() {
    }

    @SuppressWarnings("unchecked")
    public static void encode(ClientMessage clientMessage, SqlPage sqlPage) {
        clientMessage.add(BEGIN_FRAME.copy());

        byte[] content = new byte[]{(byte) (sqlPage.isLast() ? 1 : 0)};
        clientMessage.add(new ClientMessage.Frame(content));

        List<SqlColumnType> columnTypes = sqlPage.getColumnTypes();
        ListIntegerCodec.encode(clientMessage, columnTypes.stream().map(SqlColumnType::getId).collect(Collectors.toList()));

        Iterator<List<Object>> iterator = sqlPage.getColumns().iterator();

        for (SqlColumnType type : sqlPage.getColumnTypes()) {
            Object column = iterator.next();

            switch (type) {
                case VARCHAR:
                    ListMultiFrameCodec.encodeContainsNullable(clientMessage, (Collection<String>) column, StringCodec::encode);

                    break;

                case BOOLEAN:
                    // TODO
                    throw new UnsupportedOperationException("Fix");

                case TINYINT:
                    // TODO
                    throw new UnsupportedOperationException("Fix");

                case SMALLINT:
                    // TODO
                    throw new UnsupportedOperationException("Fix");

                case INTEGER:
                    ListIntegerCodec.encode(clientMessage, (Collection<Integer>) column);

                    break;

                case BIGINT:
                    ListLongCodec.encode(clientMessage, (Collection<Long>) column);

                    break;

                case DECIMAL:
                    // TODO
                    throw new UnsupportedOperationException("Fix");

                case REAL:
                    // TODO
                    throw new UnsupportedOperationException("Fix");

                case DOUBLE:
                    // TODO
                    throw new UnsupportedOperationException("Fix");

                case DATE:
                    // TODO
                    throw new UnsupportedOperationException("Fix");

                case TIME:
                    // TODO
                    throw new UnsupportedOperationException("Fix");

                case TIMESTAMP:
                    // TODO
                    throw new UnsupportedOperationException("Fix");

                case TIMESTAMP_WITH_TIME_ZONE:
                    // TODO
                    throw new UnsupportedOperationException("Fix");

                case OBJECT:
                    ListMultiFrameCodec.encode(clientMessage, (Collection<Data>) (Object) column, DataCodec::encodeNullable);

                    break;

                case NULL:
                    // TODO
                    throw new UnsupportedOperationException("Fix");

                default:
                    throw new IllegalStateException("Unknown type " + type);
            }
        }

        clientMessage.add(END_FRAME.copy());
    }

    @SuppressWarnings("unchecked")
    public static SqlPage decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        boolean isLast = iterator.next().content[0] == 1;

        ArrayList<SqlColumnType> types = ListIntegerCodec.decode(iterator).stream()
            .map(SqlColumnType::getById).collect(Collectors.toCollection(ArrayList::new));

        // TODO: (Sancar) handle empty iterator
        List<List<?>> result = new ArrayList<>(types.size());

        for (SqlColumnType type : types) {
            switch (type) {
                case VARCHAR:
                    result.add(ListMultiFrameCodec.decodeContainsNullable(iterator, StringCodec::decode));

                    break;

                case BOOLEAN:
                    // TODO
                    throw new UnsupportedOperationException("Fix");

                case TINYINT:
                    // TODO
                    throw new UnsupportedOperationException("Fix");

                case SMALLINT:
                    // TODO
                    throw new UnsupportedOperationException("Fix");

                case INTEGER:
                    result.add(ListIntegerCodec.decode(iterator));

                    break;

                case BIGINT:
                    result.add(ListLongCodec.decode(iterator));

                    break;

                case DECIMAL:
                    // TODO
                    throw new UnsupportedOperationException("Fix");

                case REAL:
                    // TODO
                    throw new UnsupportedOperationException("Fix");

                case DOUBLE:
                    // TODO
                    throw new UnsupportedOperationException("Fix");

                case DATE:
                    // TODO
                    throw new UnsupportedOperationException("Fix");

                case TIME:
                    // TODO
                    throw new UnsupportedOperationException("Fix");

                case TIMESTAMP:
                    // TODO
                    throw new UnsupportedOperationException("Fix");

                case TIMESTAMP_WITH_TIME_ZONE:
                    // TODO
                    throw new UnsupportedOperationException("Fix");

                case OBJECT:
                    result.add(ListMultiFrameCodec.decode(iterator, DataCodec::decodeNullable));

                    break;

                case NULL:
                    // TODO
                    throw new UnsupportedOperationException("Fix");

                default:
                    throw new IllegalStateException("Unknown type " + type);
            }
        }

        fastForwardToEndFrame(iterator);

        return new SqlPage(types, (List<List<Object>>) (Object) result, isLast);
    }
}
