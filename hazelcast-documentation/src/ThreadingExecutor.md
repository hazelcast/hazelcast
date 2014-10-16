
### IExecutor Threading

Executor threading is straight forward. When a task is received to be executed on Executor E, then E will have its
own `ThreadPoolExecutor` instance and the work is put on the work queue of this executor. So, Executors are fully isolated, but of course, they will share the same underlying hardware; most importantly the CPUs. 

The IExecutor can be configured using the `ExecutorConfig` (programmatic configuration) or using `<executor>` (declarative configuration).

