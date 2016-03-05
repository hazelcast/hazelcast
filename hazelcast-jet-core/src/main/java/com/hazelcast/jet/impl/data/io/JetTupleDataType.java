/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.data.io;

import com.hazelcast.jet.io.spi.DataType;
import com.hazelcast.jet.io.spi.ObjectReader;
import com.hazelcast.jet.io.spi.ObjectWriter;
import com.hazelcast.jet.io.spi.tuple.Tuple;
import com.hazelcast.jet.spi.data.tuple.JetTuple;
import com.hazelcast.jet.impl.data.tuple.DefaultJetTupleFactory;

public final class JetTupleDataType implements DataType {
    public static final DataType INSTANCE = new JetTupleDataType();

    private static final byte TYPE_ID = -4;

    private final ObjectWriter<Tuple> objectWriter =
            new JetTupleWriter();

    private final ObjectReader<Tuple> objectReader =
            new JetTupleReader(new DefaultJetTupleFactory());

    private JetTupleDataType() {
    }

    @Override
    public Class getClazz() {
        return JetTuple.class;
    }

    @Override
    public byte getTypeID() {
        return TYPE_ID;
    }

    @Override
    public ObjectWriter getObjectWriter() {
        return objectWriter;
    }

    @Override
    public ObjectReader getObjectReader() {
        return objectReader;
    }
}
