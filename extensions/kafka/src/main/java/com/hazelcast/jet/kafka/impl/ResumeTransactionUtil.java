/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.kafka.impl;

import com.hazelcast.internal.util.Preconditions;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Code adapted from Apache Flink in the
 * org.apache.flink.streaming.connectors.kafka.internal.FlinkKafkaInternalProducer
 * class.
 */
final class ResumeTransactionUtil {

    private ResumeTransactionUtil() {
    }

    /**
     * Instead of obtaining producerId and epoch from the transaction coordinator, re-use previously obtained ones,
     * so that we can resume transaction after a restart. Implementation of this method is based on
     * {@link KafkaProducer#initTransactions}.
     * https://github.com/apache/kafka/commit/5d2422258cb975a137a42a4e08f03573c49a387e
     * #diff-f4ef1afd8792cd2a2e9069cd7ddea630
     */
    static void resumeTransaction(KafkaProducer producer, long producerId, short epoch, String txnId) {
        Preconditions.checkState(producerId >= 0 && epoch >= 0,
                "Incorrect values for producerId " + producerId + " and epoch " + epoch);

        Object transactionManager = getValue(producer, "transactionManager");
        Object nextSequence = getValue(transactionManager, "nextSequence");

        invoke(transactionManager, "transitionTo",
                getEnum("org.apache.kafka.clients.producer.internals.TransactionManager$State.INITIALIZING"));
        invoke(nextSequence, "clear");

        Object producerIdAndEpoch = getValue(transactionManager, "producerIdAndEpoch");
        setValue(producerIdAndEpoch, "producerId", producerId);
        setValue(producerIdAndEpoch, "epoch", epoch);

        invoke(transactionManager, "transitionTo",
                getEnum("org.apache.kafka.clients.producer.internals.TransactionManager$State.READY"));

        invoke(transactionManager, "transitionTo",
                getEnum("org.apache.kafka.clients.producer.internals.TransactionManager$State.IN_TRANSACTION"));
        setValue(transactionManager, "transactionStarted", true);
    }

    static long getProducerId(KafkaProducer kafkaProducer) {
        Object transactionManager = getValue(kafkaProducer, "transactionManager");
        Object producerIdAndEpoch = getValue(transactionManager, "producerIdAndEpoch");
        return (long) getValue(producerIdAndEpoch, "producerId");
    }

    static short getEpoch(KafkaProducer kafkaProducer) {
        Object transactionManager = getValue(kafkaProducer, "transactionManager");
        Object producerIdAndEpoch = getValue(transactionManager, "producerIdAndEpoch");
        return (short) getValue(producerIdAndEpoch, "epoch");
    }

    @SuppressWarnings("unchecked")
    private static Enum<?> getEnum(String enumFullName) {
        String[] x = enumFullName.split("\\.(?=[^\\.]+$)");
        if (x.length == 2) {
            String enumClassName = x[0];
            String enumName = x[1];
            try {
                Class<Enum> cl = (Class<Enum>) Class.forName(enumClassName);
                return Enum.valueOf(cl, enumName);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Incompatible KafkaProducer version", e);
            }
        }
        return null;
    }

    private static Object invoke(Object object, String methodName, Object... args) {
        Class<?>[] argTypes = new Class[args.length];
        for (int i = 0; i < args.length; i++) {
            argTypes[i] = args[i].getClass();
        }
        return invoke(object, methodName, argTypes, args);
    }

    private static Object invoke(Object object, String methodName, Class<?>[] argTypes, Object[] args) {
        try {
            Method method = object.getClass().getDeclaredMethod(methodName, argTypes);
            method.setAccessible(true);
            return method.invoke(object, args);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException("Incompatible KafkaProducer version", e);
        }
    }

    private static Object getValue(Object object, String fieldName) {
        return getValue(object, object.getClass(), fieldName);
    }

    private static Object getValue(Object object, Class<?> clazz, String fieldName) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(object);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Incompatible KafkaProducer version", e);
        }
    }

    private static void setValue(Object object, String fieldName, Object value) {
        try {
            Field field = object.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(object, value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Incompatible KafkaProducer version", e);
        }
    }
}
