package com.hazelcast.jet.kafka.connect.impl;

import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

public class ReadKafkaConnectPS<T> implements ProcessorSupplier {
    private final Properties properties;
    private final EventTimePolicy<? super T> eventTimePolicy;
    private final FunctionEx<SourceRecord, T> projectionFn;
    private transient KafkaConnectorWrapper connectorWrapper;

    public ReadKafkaConnectPS(Properties properties, EventTimePolicy<? super T> eventTimePolicy,
                              FunctionEx<SourceRecord, T> projectionFn) {
        this.properties = properties;
        this.eventTimePolicy = eventTimePolicy;
        this.projectionFn = projectionFn;
    }

    public static <T> ProcessorSupplier kafkaConnectPS(@Nonnull Properties properties,
                                                       @Nonnull EventTimePolicy<? super T> eventTimePolicy,
                                                       @Nonnull FunctionEx<SourceRecord, T> projectionFn) {
        return new ReadKafkaConnectPS<>(properties, eventTimePolicy, projectionFn);
    }

    @Override
    public void init(@Nonnull Context context) {
        properties.put("tasks.max", Integer.toString(context.totalParallelism()));

        HazelcastInstance instance = context.hazelcastInstance();
        Cluster cluster = instance.getCluster();
        final UUID localMemberUUID = cluster.getLocalMember().getUuid();
        cluster.addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
            }
            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                Set<Member> members = membershipEvent.getMembers();
                Member member = members.stream()
                                       .max(comparing(Member::getUuid))
                                       .orElseThrow(() -> new IllegalStateException("Unable to determine current lead member"));
                if (localMemberUUID.equals(member.getUuid())) {
                    connectorWrapper.promoteToLeader();
                }
            }
        });

        long jobId = context.jobId();

        this.connectorWrapper = new KafkaConnectorWrapper(jobId, instance, properties);
    }

    @Override
    public void close(@Nullable Throwable error) {
        if (connectorWrapper != null) {
            connectorWrapper.stop();
        }
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        return IntStream.range(0, count)
                        .mapToObj(i -> new ReadKafkaConnectP<>(connectorWrapper, eventTimePolicy, projectionFn))
                        .collect(toList());
    }

}
