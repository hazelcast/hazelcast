# Follow the steps below to compile the shared library for Linux #

`PATH_TO_JDK_INCLUDE_DIR`: The full path for the include directory under your JDK installation.

```
gcc -c -I ${PATH_TO_JDK_INCLUDE_DIR} -fPIC -Os -o affinity_helper.o affinity_helper.c
gcc -shared -fPIC -Wl,-soname,libaffinity_helper.so -o affinity_helper.so affinity_helper.o -lc
```
