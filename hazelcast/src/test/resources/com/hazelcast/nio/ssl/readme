[commands]
keytool -genkey -alias hazelcast -keyalg RSA -keypass password -keystore hazelcast.keystore -storepass password  -validity 3600
keytool -export -alias hazelcast -file hazelcast.cer -keystore hazelcast.keystore -storepass password
keytool -import -v -trustcacerts -alias hazelcast -keypass password -file hazelcast.cer -keystore hazelcast.truststore -storepass password

[pass]
123456
