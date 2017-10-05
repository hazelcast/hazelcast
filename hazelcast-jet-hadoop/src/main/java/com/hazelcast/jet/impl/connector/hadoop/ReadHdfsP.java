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

package com.hazelcast.jet.impl.connector.hadoop;

import com.hazelcast.core.Member;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;

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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.util.Collections.emptyList;
import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.IntStream.range;
import static java.util.stream.Stream.concat;
import static org.apache.hadoop.mapred.Reporter.NULL;

/**
 * See {@link com.hazelcast.jet.core.processor.HdfsProcessors#readHdfsP(
 * org.apache.hadoop.mapred.JobConf, DistributedBiFunction)}.
 */
public final class ReadHdfsP<K, V, R> extends AbstractProcessor {

    private final Traverser<R> trav;
    private final DistributedBiFunction<K, V, R> mapper;

    private ReadHdfsP(@Nonnull List<RecordReader> recordReaders, @Nonnull DistributedBiFunction<K, V, R> mapper) {
        this.trav = traverseIterable(recordReaders).flatMap(this::traverseRecordReader);
        this.mapper = mapper;
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(trav);
    }

    private Traverser<R> traverseRecordReader(RecordReader<K, V> r) {
        return () -> {
            K key = r.createKey();
            V value = r.createValue();
            try {
                if (r.next(key, value)) {
                    return mapper.apply(key, value);
                }
                r.close();
                return null;
            } catch (IOException e) {
                throw sneakyThrow(e);
            }
        };
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    public static class MetaSupplier<K, V, R> implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final SerializableJobConf jobConf;
        private final DistributedBiFunction<K, V, R> mapper;

        private transient Map<Address, List<IndexedInputSplit>> assigned;
        private transient ILogger logger;


        public MetaSupplier(@Nonnull SerializableJobConf jobConf, @Nonnull DistributedBiFunction<K, V, R> mapper) {
            this.jobConf = jobConf;
            this.mapper = mapper;
        }

        @Override
        public void init(@Nonnull Context context) {
            logger = context.jetInstance().getHazelcastInstance().getLoggingService().getLogger(ReadHdfsP.class);
            try {
                int totalParallelism = context.totalParallelism();
                InputFormat inputFormat = jobConf.getInputFormat();
                InputSplit[] splits = inputFormat.getSplits(jobConf, totalParallelism);
                IndexedInputSplit[] indexedInputSplits = new IndexedInputSplit[splits.length];
                Arrays.setAll(indexedInputSplits, i -> new IndexedInputSplit(i, splits[i]));

                Address[] addrs = context.jetInstance().getCluster().getMembers()
                        .stream().map(Member::getAddress).toArray(Address[]::new);
                assigned = assignSplitsToMembers(indexedInputSplits, addrs);
                printAssignments(assigned);
            } catch (IOException e) {
                throw rethrow(e);
            }
        }


        @Nonnull @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new Supplier<>(
                    jobConf,
                    assigned.get(address) != null ? assigned.get(address) : emptyList(),
                    mapper);
        }

        /**
         * Heuristically solves the following problem:
         * <ul><li>
         * HDFS stores a file by dividing it into blocks and storing each block
         * on several machines.
         * </li><li>
         * To read a file, the client asks HDFS to group the blocks into a number
         * of <em>splits</em> that can be read independently. All the blocks of
         * a given split are stored on the same set of machines. The client can
         * only request the minimum number of splits, but the exact number is up
         * to HDFS.
         * </li><li>
         * {@code ReadHdfsP} acquires some splits and must plan out which splits
         * will be read by which Jet cluster member. The first concern is data
         * locality: if a split is local to a member, that member must read it.
         * Some splits may not be on any Jet member; these can be assigned
         * arbitrarily, but overall balance across members must be maintained.
         * </li><li>
         * Since each split is stored on several machines, usually there are
         * several candidate members for each split. This results in an NP
         * constraint-solving problem.
         * </li></ul>
         * This is a high-level outline of the heuristic algorithm:
         * <ol><li>
         * Build a mapping from split to the candidate set of members that might
         * read it:
         * <ol><li>
         * for each split, form the candidate set from all members which have it
         * locally;
         * </li><li>
         * for each candidate set that is still empty, replace it with a singleton
         * set containing the member that occurs in the fewest of other candidate
         * sets.
         * </li></ol>
         * </li><li>
         * Circularly iterate over all candidate sets, removing from each
         * non-singleton set the member that occurs in the largest number of other
         * candidate sets.
         * </li></ol>
         */
        private Map<Address, List<IndexedInputSplit>> assignSplitsToMembers(
                IndexedInputSplit[] indexedSplits, Address[] memberAddrs
        ) {
            Map<IndexedInputSplit, Set<Integer>> splitToCandidates = new TreeMap<>();
            int[] memberToSplitCount = new int[memberAddrs.length];

            // Each member that has the split locally is a candidate
            for (IndexedInputSplit is : indexedSplits) {
                splitToCandidates.put(is,
                        range(0, memberAddrs.length)
                                .filter(i -> isSplitLocalForMember(is.getSplit(), memberAddrs[i]))
                                .peek(i -> memberToSplitCount[i]++)
                                .boxed()
                                .collect(toSet())
                );
            }
            // for each split not local to any member, assign it to the member
            // with the least splits assigned so far
            splitToCandidates.entrySet().stream()
                    .filter(e -> e.getValue().isEmpty())
                    .peek(e -> logger.info(
                            "No local member found for " + e.getKey() + ", will be read remotely."))
                    .map(Entry::getValue)
                    .forEach(memberIndexes -> {
                        int target = indexOfMin(memberToSplitCount);
                        memberIndexes.add(target);
                        memberToSplitCount[target]++;
                    });
            logger.info("Split counts per member before uniquifying: " + Arrays.toString(memberToSplitCount));

            // decide on a unique member for each split
            boolean[] foundNonUnique = new boolean[1];
            do {
                foundNonUnique[0] = false;
                splitToCandidates
                        .values().stream()
                        .filter(memberIndexes -> memberIndexes.size() > 1)
                        .peek(x -> foundNonUnique[0] = true)
                        .forEach(memberIndexes -> {
                            int memberWithMostSplits = memberIndexes
                                    .stream()
                                    .max(comparingInt(i -> memberToSplitCount[i]))
                                    .get();
                            memberIndexes.remove(memberWithMostSplits);
                            memberToSplitCount[memberWithMostSplits]--;
                        });
            } while (foundNonUnique[0]);
            logger.info("Final split counts per member: " + Arrays.toString(memberToSplitCount));
            return splitToCandidates.entrySet().stream()
                    .map(e -> entry(e.getKey(), memberAddrs[singleItem(e.getValue())]))
                    .collect(groupingBy(Entry::getValue, mapping(Entry::getKey, toList())));
        }

        private void printAssignments(Map<Address, List<IndexedInputSplit>> assigned) {
            logger.info("Member-to-split assignment: " +
                    assigned.entrySet().stream().flatMap(e -> concat(
                            Stream.of(e.getKey() + ":"),
                            Optional.of(e.getValue()).orElse(emptyList()).stream().map(Object::toString))
                    ).collect(joining("\n")));
        }

        private static boolean isSplitLocalForMember(InputSplit split, Address memberAddr) {
            try {
                final InetAddress inetAddr = memberAddr.getInetAddress();
                return Arrays.stream(split.getLocations())
                        .flatMap(loc -> Arrays.stream(uncheckCall(() -> InetAddress.getAllByName(loc))))
                        .anyMatch(inetAddr::equals);
            } catch (IOException e) {
                throw sneakyThrow(e);
            }
        }

        private static Integer indexOfMin(int[] ints) {
            return range(0, ints.length)
                    .boxed()
                    .min(comparingInt(i -> ints[i]))
                    .get();
        }

        private static <T> T singleItem(Collection<T> coll) {
            if (coll.size() != 1) {
                throw new AssertionError("Collection does not have exactly one item: " + coll);
            }
            return coll.iterator().next();
        }
    }

    private static class Supplier<K, V, R> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private SerializableJobConf jobConf;
        private List<IndexedInputSplit> assignedSplits;
        private DistributedBiFunction<K, V, R> mapper;

        Supplier(SerializableJobConf jobConf,
                 Collection<IndexedInputSplit> assignedSplits,
                 @Nonnull DistributedBiFunction<K, V, R> mapper
        ) {
            this.jobConf = jobConf;
            this.assignedSplits = new ArrayList<>(assignedSplits);
            this.mapper = mapper;
        }

        @Override
        @Nonnull
        public List<Processor> get(int count) {
            Map<Integer, List<IndexedInputSplit>> processorToSplits =
                    range(0, assignedSplits.size()).mapToObj(i -> new SimpleImmutableEntry<>(i, assignedSplits.get(i)))
                            .collect(groupingBy(e -> e.getKey() % count,
                                    mapping(Entry::getValue, toList())));
            range(0, count)
                    .forEach(processor -> processorToSplits.computeIfAbsent(processor, x -> emptyList()));
            InputFormat inputFormat = jobConf.getInputFormat();

            return processorToSplits
                    .values().stream()
                    .map(splits -> splits.isEmpty()
                            ? Processors.noopP().get()
                            : new ReadHdfsP<>(splits.stream()
                            .map(IndexedInputSplit::getSplit)
                            .map(split -> uncheckCall(() ->
                                    inputFormat.getRecordReader(split, jobConf, NULL)))
                            .collect(toList()), mapper)
                    ).collect(toList());
        }

        private void writeObject(ObjectOutputStream out) throws IOException {
            jobConf.write(out);
            out.writeObject(assignedSplits);
            out.writeObject(mapper);
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            jobConf = new SerializableJobConf();
            jobConf.readFields(in);
            assignedSplits = (List<IndexedInputSplit>) in.readObject();
            mapper = (DistributedBiFunction<K, V, R>) in.readObject();
        }
    }

    /**
     * Wrapper of {@code InputSplit} that adds serializability and sortability
     * by the position of the split in the HDFS file.
     */
    private static class IndexedInputSplit implements Comparable<IndexedInputSplit>, Serializable {

        private int index;
        private InputSplit split;

        IndexedInputSplit(int index, InputSplit split) {
            this.index = index;
            this.split = split;
        }

        InputSplit getSplit() {
            return split;
        }

        @Override
        public String toString() {
            try {
                return "IndexedInputSplit{index " + index + ", blocks " + blocksOfSplit(split)
                        + ", locations " + Arrays.toString(split.getLocations()) + '}';
            } catch (IOException e) {
                throw rethrow(e);
            }
        }

        @Override
        public int compareTo(@Nonnull IndexedInputSplit other) {
            return Integer.compare(index, other.index);
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

        private static String blocksOfSplit(InputSplit split) {
            final String s = split.toString();
            return s.substring(s.lastIndexOf(':') + 1);
        }
    }
}
