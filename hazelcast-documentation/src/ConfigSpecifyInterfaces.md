

### Specifying Network Interfaces

You can also specify which network interfaces that Hazelcast should use. Servers usually have more than one network interface, so you may want to list the valid IPs. You can use range characters ('\*' and '-') for simplicity. For instance, 10.3.10.\* refers to IPs between 10.3.10.0 and 10.3.10.255. Interface 10.3.10.4-18 refers to IPs between 10.3.10.4 and 10.3.10.18 (4 and 18 included). If the network interface configuration is enabled (it is disabled by default) and if Hazelcast cannot find a matching interface, then it will print a message on the console and won't start on that node.

```xml
<hazelcast>
  ...
  <network>
    ...
    <interfaces enabled="true">
      <interface>10.3.16.*</interface> 
      <interface>10.3.10.4-18</interface> 
      <interface>192.168.1.3</interface>         
    </interfaces>    
  </network>
  ...
</hazelcast> 
```
<br></br>