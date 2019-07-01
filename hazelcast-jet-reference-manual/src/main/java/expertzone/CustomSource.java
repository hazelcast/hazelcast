/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package expertzone;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.DiagnosticProcessors;
import com.hazelcast.nio.Address;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

class S1 {
    static
    //tag::s1[]
    class GenerateNumbersP extends AbstractProcessor {

        private final Traverser<Integer> traverser;

        GenerateNumbersP(int upperBound) {
            traverser = Traversers.traverseStream(range(0, upperBound).boxed());
        }

        @Override
        public boolean complete() {
            return emitFromTraverser(traverser);
        }
    }
    //end::s1[]

    static void main() {
        //tag::s2[]
        JetInstance jet = Jet.newJetInstance();

        int upperBound = 10;
        DAG dag = new DAG();
        Vertex generateNumbers = dag.newVertex("generate-numbers",
                () -> new GenerateNumbersP(upperBound));
        Vertex logInput = dag.newVertex("log-input",
                DiagnosticProcessors.writeLoggerP(i -> "Received number: " + i));
        dag.edge(Edge.between(generateNumbers, logInput));

        try {
            jet.newJob(dag).join();
        } finally {
            Jet.shutdownAll();
        }
        //end::s2[]
    }
}

class GenerateNumbersP extends AbstractProcessor {
    private final Traverser<Integer> traverser;

    //tag::s3[]
    GenerateNumbersP(int upperBound, int processorCount, int processorIndex) {
        traverser = Traversers.traverseStream(
                range(0, upperBound)
                         .filter(n -> n % processorCount == processorIndex)
                         .boxed());

    }
    //end::s3[]
}

//tag::s4[]
class GenerateNumbersPSupplier implements ProcessorSupplier {

    private final int upperBound;

    GenerateNumbersPSupplier(int upperBound) {
        this.upperBound = upperBound;
    }

    @Override @Nonnull
    public List<? extends Processor> get(int processorCount) {
        return
                range(0, processorCount)
                .mapToObj(index -> new GenerateNumbersP(
                        upperBound, processorCount, index))
                .collect(toList());
    }
}
//end::s4[]

class S5 {
    static void s5() {
        //tag::s5[]
        DAG dag = new DAG();
        Vertex generateNumbers = dag.newVertex("generate-numbers",
                new GenerateNumbersPSupplier(10));
        Vertex logInput = dag.newVertex("log-input",
                DiagnosticProcessors.writeLoggerP(i -> "Received number: " + i));
        dag.edge(Edge.between(generateNumbers, logInput));
        //end::s5[]
    }

    static void s6() {
        //tag::s6[]
        JetInstance jet = Jet.newJetInstance();
        Jet.newJetInstance();

        int upperBound = 10;
        DAG dag = new DAG();
        // rest of the code same as above
        //end::s6[]
    }
}

//tag::s7[]
class GenerateNumbersPMetaSupplier implements ProcessorMetaSupplier {

    private final int upperBound;

    private transient int totalParallelism;
    private transient int localParallelism;

    GenerateNumbersPMetaSupplier(int upperBound) {
        this.upperBound = upperBound;
    }

    @Override
    public void init(@Nonnull Context context) {
        totalParallelism = context.totalParallelism();
        localParallelism = context.localParallelism();
    }

    @Override @Nonnull
    public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
        Map<Address, ProcessorSupplier> map = new HashMap<>();
        for (int i = 0; i < addresses.size(); i++) {
            // We'll calculate the global index of each processor in the cluster:
            int globalIndexBase = localParallelism * i;
            // Capture the value of the transient field for the lambdas below:
            int divisor = totalParallelism;
            // processorCount will be equal to localParallelism:
            ProcessorSupplier supplier = processorCount ->
                    range(globalIndexBase, globalIndexBase + processorCount)
                            .mapToObj(globalIndex ->
                                    new GenerateNumbersP(upperBound, divisor, globalIndex)
                            ).collect(toList());
            map.put(addresses.get(i), supplier);
        }
        return map::get;
    }
}
//end::s7[]

class S8 {
    static void s8() {
        int upperBound = 10;
        //tag::s8[]
        DAG dag = new DAG();
        Vertex generateNumbers = dag.newVertex("generate-numbers",
                new GenerateNumbersPMetaSupplier(upperBound));
        Vertex logInput = dag.newVertex("log-input",
                DiagnosticProcessors.writeLoggerP(i -> "Received number: " + i));
        dag.edge(Edge.between(generateNumbers, logInput));
        //end::s8[]
    }
}
