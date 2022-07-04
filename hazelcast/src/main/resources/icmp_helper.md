# Follow the steps below to compile the shared library for Linux #

`PATH_TO_JDK_INCLUDE_DIR`: The full path for the include directory under your JDK installation.

```
gcc -c -I ${PATH_TO_JDK_INCLUDE_DIR} -fPIC -o icmp_helper.o icmp_helper.c
gcc -shared -fPIC -Wl,-soname,libicmp_helper.so -o libicmp_helper.so icmp_helper.o -lc
```
