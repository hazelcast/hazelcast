/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.connector.hadoop;

import com.hazelcast.core.Member;
import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.Processors.NoopProcessor;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TextInputFormat;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.util.Collections.emptyList;
import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static java.util.stream.Stream.concat;
import static org.apache.hadoop.mapred.Reporter.NULL;

/**
 * HDFS reader for Jet, emits records read from HDFS file as Map.Entry
 */
public final class HdfsReader extends AbstractProcessor {

    private static final ILogger LOGGER = Logger.getLogger(HdfsReader.class);

    private final List<RecordReader> recordReaders;

    private HdfsReader(List<RecordReader> recordReaders) {
        this.recordReaders = recordReaders;
    }

    @Override
    public boolean complete() {
        try {
            Iterator<RecordReader> iterator = recordReaders.iterator();
            while (iterator.hasNext()) {
                RecordReader recordReader = iterator.next();
                boolean read;
                do {
                    Object key = recordReader.createKey();
                    Object value = recordReader.createValue();
                    read = recordReader.next(key, value);
                    if (read) {
                        emit(new SimpleImmutableEntry<>(key, value));
                        if (getOutbox().isHighWater()) {
                            return false;
                        }
                    }

                } while (read);
                recordReader.close();
                iterator.remove();
            }
            return true;
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    /**
     * Creates supplier for reading HDFS files.
     *
     * @param path input path for reading files
     * @return {@link ProcessorMetaSupplier} supplier
     */
    public static ProcessorMetaSupplier supplier(String path) {
        return new MetaSupplier(path);
    }

    private static class MetaSupplier implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final String path;
        private transient Map<Address, Collection<IndexedInputSplit>> assigned;
        private transient JobConf configuration;


        MetaSupplier(String path) {
            this.path = path;
        }

        @Override
        public void init(@Nonnull Context context) {
            configuration = new JobConf();
            configuration.setInputFormat(TextInputFormat.class);
            TextInputFormat.addInputPath(configuration, new Path(path));
            try {
                int totalParallelism = context.totalParallelism();
                InputSplit[] splits = configuration.getInputFormat().getSplits(configuration, totalParallelism);
                IndexedInputSplit[] indexedInputSplits = new IndexedInputSplit[splits.length];
                Arrays.setAll(indexedInputSplits, i -> new IndexedInputSplit(i, splits[i]));

                Set<Member> members = context.jetInstance().getCluster().getMembers();
                assigned = assignSplits(indexedInputSplits, members.toArray(new Member[members.size()]));
                printAssignments(assigned);
            } catch (IOException e) {
                throw rethrow(e);
            }
        }


        @Override @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address ->
                    new Supplier(configuration, assigned.get(address) != null ? assigned.get(address) : emptyList());
        }

        private static void printAssignments(Map<Address, Collection<IndexedInputSplit>> assigned) {
            LOGGER.info(assigned.entrySet().stream().flatMap(e -> concat(
                    Stream.of(e.getKey() + ":"),
                    Optional.of(e.getValue()).orElse(emptyList()).stream().map(Object::toString))
            ).collect(joining("\n")));
        }

        private static boolean isSplitLocalForMember(Member member, InputSplit split) throws IOException {
            final InetAddress memberAddr = member.getAddress().getInetAddress();
            return Arrays.stream(split.getLocations())
                    .flatMap(loc -> Arrays.stream(uncheckCall(() -> InetAddress.getAllByName(loc))))
                    .anyMatch(memberAddr::equals);
        }

        private static Map<Address, Collection<IndexedInputSplit>> assignSplits(
                IndexedInputSplit[] inputSplits, Member[] members) throws IOException {
            Map<IndexedInputSplit, List<Integer>> assignments = new TreeMap<>();
            int[] counts = new int[members.length];

            // assign local members
            for (IndexedInputSplit inputSplit : inputSplits) {
                List<Integer> indexes = new ArrayList<>();
                for (int i = 0; i < members.length; i++) {
                    if (isSplitLocalForMember(members[i], inputSplit.getSplit())) {
                        indexes.add(i);
                        counts[i]++;
                    }
                }
                assignments.put(inputSplit, indexes);
            }
            // assign all remaining splits to member with lowest number of splits
            for (Map.Entry<IndexedInputSplit, List<Integer>> entry : assignments.entrySet()) {
                List<Integer> indexes = entry.getValue();
                if (indexes.isEmpty()) {
                    int indexToAdd = range(0, counts.length).boxed().min(comparingInt(i -> counts[i]))
                                                            .orElseThrow(() -> new AssertionError("Empty counts"));
                    indexes.add(indexToAdd);
                    counts[indexToAdd]++;
                }
            }
            LOGGER.info("Counts before pruning: " + Arrays.toString(counts));

            // prune addresses for splits with more than one member assigned
            boolean found;
            do {
                found = false;
                for (Map.Entry<IndexedInputSplit, List<Integer>> entry : assignments.entrySet()) {
                    List<Integer> indexes = entry.getValue();
                    if (indexes.size() > 1) {
                        found = true;
                        // find member with most splits and remove from list
                        int indexWithMaxCount = indexes.stream().max(comparingInt(i -> counts[i]))
                                                       .orElseThrow(() -> new AssertionError("Empty indexes"));
                        // remove the item in the list which has the value "indexWithMaxCount"
                        indexes.remove(Integer.valueOf(indexWithMaxCount));
                        counts[indexWithMaxCount]--;
                    }
                }
            } while (found);

            LOGGER.info("Final counts=" + Arrays.toString(counts));
            // assign to map
            Map<Address, Collection<IndexedInputSplit>> mapToSplit = new HashMap<>();
            for (Map.Entry<IndexedInputSplit, List<Integer>> entry : assignments.entrySet()) {
                IndexedInputSplit split = entry.getKey();
                List<Integer> indexes = entry.getValue();

                if (indexes.size() != 1) {
                    throw new RuntimeException("Split " + split + " has " + indexes.size() + " assignments");
                }

                // add input split to final assignment list
                Integer memberIndex = indexes.get(0);
                Address address = members[memberIndex].getAddress();
                Collection<IndexedInputSplit> assigned = mapToSplit.get(address);
                if (assigned != null) {
                    assigned.add(split);
                } else {
                    mapToSplit.put(address, new TreeSet<>(Collections.singletonList(split)));
                }
            }
            return mapToSplit;
        }

    }

    private static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private JobConf configuration;
        private List<IndexedInputSplit> assignedSplits;

        Supplier(JobConf configuration, Collection<IndexedInputSplit> assignedSplits) {
            this.configuration = configuration;
            this.assignedSplits = assignedSplits.stream().collect(toList());
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            Map<Integer, List<IndexedInputSplit>> processorToSplits = range(0, assignedSplits.size()).boxed()
                    .map(i -> new SimpleImmutableEntry<>(i, assignedSplits.get(i)))
                    .collect(groupingBy(e -> e.getKey() % count, mapping(Map.Entry::getValue, toList())));
            range(0, count)
                    .forEach(processor -> processorToSplits.computeIfAbsent(processor, x -> emptyList()));
            InputFormat inputFormat = configuration.getInputFormat();
            return processorToSplits
                    .values().stream()
                    .map(splits -> splits.isEmpty()
                            ? new NoopProcessor()
                            : new HdfsReader(splits.stream()
                                                   .map(IndexedInputSplit::getSplit)
                                                   .map(split -> uncheckCall(() ->
                                                           inputFormat.getRecordReader(split, configuration, NULL)))
                                                   .collect(toList()))
                    ).collect(toList());
        }

        private void writeObject(ObjectOutputStream out) throws IOException {
            configuration.write(out);
            out.writeObject(assignedSplits);
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            configuration = new JobConf();
            configuration.readFields(in);
            assignedSplits = (List<IndexedInputSplit>) in.readObject();
        }
    }

    private static class IndexedInputSplit implements Comparable<IndexedInputSplit>, Serializable {

        private int index;
        private InputSplit split;

        IndexedInputSplit(int index, InputSplit split) {
            this.index = index;
            this.split = split;
        }

        public InputSplit getSplit() {
            return split;
        }

        @Override
        public String toString() {
            return "IndexedInputSplit{index=" + index + ", split=" + split + '}';
        }

        @Override
        public int compareTo(IndexedInputSplit o) {
            return Integer.compare(index, o.index);
        }

        @Override
        public boolean equals(Object o) {
            IndexedInputSplit that;
            return this == o ||
                    o != null
                    && getClass() == o.getClass()
                    && index == (that = (IndexedInputSplit) o).index
                    && Objects.equals(split, that.split);
        }

        @Override
        public int hashCode() {
            return 31 * index + Objects.hashCode(split);
        }

        private void writeObject(ObjectOutputStream out) throws IOException {
            out.writeInt(index);
            out.writeUTF(split.getClass().getName());
            split.write(out);
        }

        private void readObject(ObjectInputStream in) throws Exception {
            index = in.readInt();
            split = ClassLoaderUtil.newInstance(null, in.readUTF());
            split.readFields(in);
        }
    }
}
