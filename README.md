# Confluent Proactive Support: Client

# Overview

This repository contains the client application for metrics collection of proactive support.


# Development

## Requirements

This project requires Kafka 0.9 built against Scala 2.11, which as of 02-Nov-2015 is not yet officially released.
You must therefore manually build Kafka and install it to your local Maven repository:

```shell
# Install Kafka trunk/master (w/ Scala 2.11) to local maven directory
$ git clone git@github.com:confluentinc/kafka.git && cd kafka
$ ./gradlew -PscalaVersion=2.11.7 clean install
```


## Building

This project uses the standard maven lifecycles such as:

```shell
$ mvn compile
$ mvn test
$ mvn package # creates the jar file
```


# References

* [support-metrics-server](https://github.com/confluentinc/support-metrics-server)
  -- the corresponding server application for this client application
