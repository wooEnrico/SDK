# SDK
Java Software Development Kit


# USEAGE

### maven
```xml
        <dependency>
            <groupId>io.github.wooenrico</groupId>
            <artifactId>tools</artifactId>
            <version>0.0.1</version>
        </dependency>
```

### gradle
```xml
    implementation 'io.github.wooenrico:tools:0.0.1'
```

### main test 
```java
    public static void main(String[] args) throws UnknownHostException {
        String inet4HostName = io.github.wooenrico.NetUtils.getInet4HostName();
        System.out.println(inet4HostName);
    }
```

# [JAVA DOCS](https://wooenrico.github.io/SDK/)
