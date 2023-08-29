# OS requirements

The hazelcast-tpc-engine-iouring can only be build and run on Linux and
requires a 64 bit kernel with version of 5.7 or higher. Currently there 
is only a X86_64 build, in the future an ARM build needs to be added.

# Libraries

When using hazelcast-tpc-engine-iouring, you also need to have liburing
installed.

For Ubuntu, Debian:

```
sudo apt-get install liburing
```

For RHEL, Fedora:

```
sudo yum install liburing
```

For Arch Linux:

```
sudo pacman -S liburing
```

# Starup problems

If you run into uring startup problems because memory can't be acquired for
uring, first run:

```
ulimit -l
```

To show the memlock limit. And probably it will show something like

```
8196
```

To permanently increase this limit, modify the following file:
```
/etc/security/limits.conf
```
And set the following values:

```
<username>  soft memlock 102400
<username>  hard memlock 102400
```

And replace <username> by the user that is running Hazelcast. This will reserve
102400 bytes. After the file is modified, reboot.

You can also set it to unlimited:

```
<username>  soft memlock unlimited
<username>  hard memlock unlimited
```