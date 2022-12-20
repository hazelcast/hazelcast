# Running benchmarks 

Each benchmark contains ```main``` method which gives you an option to run a single benchmark.
To run a benchmark with profilers you can modify the ```main``` method. Examples below.

## No profiler

```java
public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
            .include(MultipleWritesBenchmark.class.getSimpleName())
            .build();

    new Runner(opt).run();
}
```

## Perf

```java
public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
            .include(MultipleWritesBenchmark.class.getSimpleName())
            .addProfiler(LinuxPerfProfiler.class)
            .build();

    new Runner(opt).run();
}
```

## Other one-liner profilers

- ```LinuxPerfAsmProfiler``` - shows you an assembly of your code, useful to detect if hot loop is 
  vectorised - you need to have JDK with ```hsdis-*.so``` installed
- ```LinuxPerfNormProfiler``` - shows normalized version of perf profile
- ```LinuxPerfC2CProfiler``` - shows C2C perf module basic stats 

## Async-profiler bundled

```java
public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
            .include(MultipleWritesBenchmark.class.getSimpleName())
            .addProfiler(AsyncProfiler.class, "output=jfr;event=cpu")
            .build();

    new Runner(opt).run();
}
```

## Async-profiler external

```java
public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
            .include(MultipleWritesBenchmark.class.getSimpleName())
            .addProfiler(AsyncProfiler.class, "output=jfr;event=cpu;libPath=/path/to/libasyncProfiler.so")
            .build();

    new Runner(opt).run();
}
```
# Running all the benchmarks

You need to build the project with maven profile ```benchmark```:

```shell
mvn -Dquick -Pbenchmark clean package
```

And run:

```shell
java -cp "$HOME/.m2/repository/org/openjdk/jmh/jmh-generator-annprocess/1.34/jmh-generator-annprocess-1.34.jar:$HOME/.m2/repository/org/openjdk/jmh/jmh-core/1.34/jmh-core-1.34.jar:hazelcast-perf-tests/target/hazelcast-perf-tests-5.3.0-SNAPSHOT-tests.jar:hazelcast/target/hazelcast-5.3.0-SNAPSHOT.jar" com.hazelcast.internal.perf.BenchmarkRunner
```

In later stage we can modify the ```BenchmarkRunner``` class to run just a set of benchmarks. 