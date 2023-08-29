If you run into uring startup problems because memory can't be acquired for
uring, first run:

ulimit -l

To show the memlock limit. And probably it will show something like

8196

To permanently increase this limit, modify the following file:

/etc/security/limits.conf

And set the following values:

<username>  soft memlock 102400
<username>  hard memlock 102400

And replace <username> by the user that is running Hazelcast. This will reserve
102400 bytes. After the file is modified, reboot.

You can also set it to unlimited:

<username>  soft memlock unlimited
<username>  hard memlock unlimited