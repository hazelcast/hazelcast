


### Enable the Class

Now, we need to enable the class `CounterService`. Declarative way of doing this is shown below.

```xml
<network>
   <join><multicast enabled="true"/> </join>
</network>
<services>
   <service enabled="true">
      <name>CounterService</name>
      <class-name>CounterService</class-name>
   </service>
</services>
```

`CounterService` is declared within the `services` tag of configuration. 

- Setting the `enabled` attribute as `true` enables the service.
- `name` attribute defines the name of the service. It should be a unique name (`CounterService` in our case) since it will be looked up when a remote call is made. Note that, the value of this attribute will be sent at each request. So, a longer value means more data (de)serialization. A good practice is giving an understandable name with the shortest possible length.
- `class-name`: Class name of the service (`CounterService` in our case). Class should have a *no-arg* constructor. Otherwise, the object cannot be initialized.

Moreover, note that multicast is enabled as the join mechanism. In the later sections, we will see why.

