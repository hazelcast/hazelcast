package com.hazelcast.client.impl.protocol.template;

import com.hazelcast.annotation.GenerateCodec;
import com.hazelcast.annotation.Nullable;
import com.hazelcast.annotation.Request;
import com.hazelcast.client.impl.protocol.ResponseMessageConst;
import com.hazelcast.nio.serialization.Data;

import java.util.List;

@GenerateCodec(id = TemplateConstants.RINGBUFFER_TEMPLATE_ID, name = "Ringbuffer", ns = "Hazelcast.Client.Protocol.Ringbuffer")
public interface RingbufferCodecTemplate {

    @Request(id = 1, retryable = false, response = ResponseMessageConst.LONG)
    void size(String name);

    @Request(id = 2, retryable = false, response = ResponseMessageConst.LONG)
    void tailSequence(String name);

    @Request(id = 3, retryable = false, response = ResponseMessageConst.LONG)
    void headSequence(String name);

    @Request(id = 4, retryable = false, response = ResponseMessageConst.LONG)
    void capacity(String name);

    @Request(id = 5, retryable = false, response = ResponseMessageConst.LONG)
    void remainingCapacity(String name);

    @Request(id = 6, retryable = false, response = ResponseMessageConst.LONG)
    void add(String name, int overflowPolicy, Data value);

    @Request(id = 7, retryable = false, response = ResponseMessageConst.LONG)
    void addAsync(String name, int overflowPolicy, Data value);

    @Request(id = 8, retryable = false, response = ResponseMessageConst.DATA)
    void readOne(String name, long sequence);

    @Request(id = 9, retryable = false, response = ResponseMessageConst.LONG)
    void addAllAsync(String name, List<Data> valueList, int overflowPolicy);

    @Request(id = 10, retryable = false, response = ResponseMessageConst.READ_RESULT_SET)
    void readManyAsync(String name, long startSequence, int minCount, int maxCount, @Nullable Data filter);
}
