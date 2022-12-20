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
