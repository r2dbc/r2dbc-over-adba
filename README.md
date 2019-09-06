# Reactive Relational Database Connectivity over ADBA Experiment

:warning: **This project is currently no longer maintained given the lack in movement in the ADBA project.** :warning:

We might reconsider reviving this project once ADBA has made significant progress.

This project contains the [ADBA][a] Adapter of the [R2DBC SPI][r] that allows usage of ADBA drivers by using the R2DBC SPI. This implementation is not intended to be used directly, but rather to be used as the backing implementation for a humane client library to delegate to.

[a]: https://github.com/pull-vert/adba-mirror
[r]: https://github.com/r2dbc/r2dbc-spi

## Maven
Both milestone and snapshot artifacts (library, source, and javadoc) can be found in Maven repositories.

```xml
<dependency>
  <groupId>io.r2dbc</groupId>
  <artifactId>r2dbc-over-adba</artifactId>
  <version>1.0.0.BUILD-SNAPSHOT</version>
</dependency>
```

Artifacts can bound found at the following repositories.

### Repositories
```xml
<repository>
    <id>spring-snapshots</id>
    <name>Spring Snapshots</name>
    <url>https://repo.spring.io/snapshot</url>
    <snapshots>
        <enabled>true</enabled>
    </snapshots>
</repository>
```

```xml
<repository>
    <id>spring-milestones</id>
    <name>Spring Milestones</name>
    <url>https://repo.spring.io/milestone</url>
    <snapshots>
        <enabled>false</enabled>
    </snapshots>
</repository>
```

## License
This project is released under version 2.0 of the [Apache License][l].

[l]: https://www.apache.org/licenses/LICENSE-2.0
