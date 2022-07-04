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
import org.apache.kafka.clients.producer.internals.TransactionManager;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Code adapted from Apache Flink in the
 * org.apache.flink.streaming.connectors.kafka.internal.FlinkKafkaInternalProducer
 * class.
 */
final class ResumeTransactionUtil {

    private static final String TRANSACTION_MANAGER_FIELD_NAME = "transactionManager";
    private static final String TRANSACTION_MANAGER_STATE_ENUM =
            "org.apache.kafka.clients.producer.internals.TransactionManager$State";
    private static final String PRODUCER_ID_AND_EPOCH_FIELD_NAME = "producerIdAndEpoch";

    private ResumeTransactionUtil() {
    }

    /**
     * Instead of obtaining producerId and epoch from the transaction coordinator, re-use previously
     * obtained ones, so that we can resume transaction after a restart. Implementation of this
     * method is based on https://github.com/apache/flink/blob/577648379c2abb429259ac1a46ca6a04550f3dbd/flink-connectors
     * /flink-connector-kafka/src/main/java/org/apache/flink/connector/kafka/sink/FlinkKafkaInternalProducer.java#L276-L308
     */
    static void resumeTransaction(KafkaProducer producer, long producerId, short epoch) {
        Preconditions.checkState(producerId >= 0 && epoch >= 0,
                "Incorrect values for producerId " + producerId + " and epoch " + epoch);

        Object transactionManager = getTransactionManager(producer);
        synchronized (transactionManager) {
            Object topicPartitionBookkeeper =
                    getField(transactionManager, "topicPartitionBookkeeper");

            transitionTransactionManagerStateTo(transactionManager, "INITIALIZING");
            invoke(topicPartitionBookkeeper, "reset");

            setField(
                    transactionManager,
                    PRODUCER_ID_AND_EPOCH_FIELD_NAME,
                    createProducerIdAndEpoch(producerId, epoch));

            transitionTransactionManagerStateTo(transactionManager, "READY");

            transitionTransactionManagerStateTo(transactionManager, "IN_TRANSACTION");
            setField(transactionManager, "transactionStarted", true);
        }
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

    private static Object createProducerIdAndEpoch(long producerId, short epoch) {
        try {
            Field field =
                    TransactionManager.class.getDeclaredField(PRODUCER_ID_AND_EPOCH_FIELD_NAME);
            Class<?> clazz = field.getType();
            Constructor<?> constructor = clazz.getDeclaredConstructor(Long.TYPE, Short.TYPE);
            constructor.setAccessible(true);
            return constructor.newInstance(producerId, epoch);
        } catch (InvocationTargetException
                | InstantiationException
                | IllegalAccessException
                | NoSuchFieldException
                | NoSuchMethodException e) {
            throw new RuntimeException("Incompatible KafkaProducer version", e);
        }
    }

    private static Object getTransactionManager(KafkaProducer kafkaProducer) {
        return getField(kafkaProducer, TRANSACTION_MANAGER_FIELD_NAME);
    }

    /**
     * Gets and returns the field {@code fieldName} from the given Object {@code object} using
     * reflection.
     */
    private static Object getField(Object object, String fieldName) {
        return getField(object, object.getClass(), fieldName);
    }

    /**
     * Gets and returns the field {@code fieldName} from the given Object {@code object} using
     * reflection.
     */
    private static Object getField(Object object, Class<?> clazz, String fieldName) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(object);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Incompatible KafkaProducer version", e);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Enum<?> getTransactionManagerState(String enumName) {
        try {
            Class<Enum> cl = (Class<Enum>) Class.forName(TRANSACTION_MANAGER_STATE_ENUM);
            return Enum.valueOf(cl, enumName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Incompatible KafkaProducer version", e);
        }
    }

    private static void transitionTransactionManagerStateTo(
            Object transactionManager, String state) {
        invoke(transactionManager, "transitionTo", getTransactionManagerState(state));
    }

    /**
     * Sets the field {@code fieldName} on the given Object {@code object} to {@code value} using
     * reflection.
     */
    private static void setField(Object object, String fieldName, Object value) {
        setField(object, object.getClass(), fieldName, value);
    }

    private static void setField(Object object, Class<?> clazz, String fieldName, Object value) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(object, value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Incompatible KafkaProducer version", e);
        }
    }
}
