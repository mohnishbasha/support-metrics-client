.. _ps_intro:

Proactive Support
================

Proactive Support is a module that ships with the Confluent Platform. It collects information from a running cluster and periodically sends that information to Confluent. Confluent uses the data to diagnose customers problems and to build better products by identifying problems in the field early on. Proactive Support provides two levels of data collection: basic and enhanced. Basic data collection is performed for any customer of the platform. Enhanced data collection is performed for licensed customers.

Basic anonymous data collection
----------
Basic data collection is a feature of Proactive Support that sends basic metrics from each broker to Confluent. It is enabled by default for anyone that downloads the Confluent Platform. The metrics collected are sent anonymously to Confluent.


What metrics are collected?
~~~~~~~~~~~~~~~~~~~~~~
Each broker in the cluster collects the following metrics:

* **Timestamp** - Time when this data record was created on the broker.
* **Kafka version** - Apache Kafka version this broker is running.
* **Confluent Platform version** - Confluent Platform version this broker is running.
* **Broker ID** - A unique identifier that is valid for the runtime of a broker.  The identifier is generated at broker startup and lost at shutdown.

Configuration: enabling or disabling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The default broker configuration file that comes with the Confluent Platform enables basic anonymous data collection. We describe the fields included in the configuration file here, including how to disable data collection:


.. sourcecode:: bash
    :linenos:

            ############################ Confluent Support ####################

            # The customer ID under which metrics will be collected and reported.
            #
            # When the customer ID is set to "anonymous" (the default), then only
            # a reduced set of metrics is being collected and reported.
            #
            # If you are a Confluent customer, then you should replace the default value
            # with your actual Confluent customer ID.  Doing so will ensure that further
            # support-relevant metrics will be collected and reported.
            confluent.support.customer.id=anonymous

            # The interval at which metrics will be collected from and reported by this
            # broker.
            confluent.support.metrics.report.interval.hours=24

            # The Kafka topic (within the same cluster as this broker) to which
            # support metrics will be submitted.
            #
            # To disable the collection of support metrics to an internal Kafka topic
            # set this variable to an empty value.
            confluent.support.metrics.topic=__confluent.support.metrics

            # Endpoint for secure transmission of support metrics to Confluent.
            #
            # The secure endpoint takes precedence over the insecure endpoint, i.e. if the
            # secure endpoint is enabled, then metrics will not be transmitted via the
            # insecure endpoint.  If the secure endpoint is not reachable, metrics
            # transmission falls back to the insecure endpoint (if enabled).
            #
            # To disable sharing metrics over the secure endpoint set this variable to an
            # empty value.
            #
            # Note: To disable the sharing of any metrics with Confluent you must disable both
            #       the secure and the insecure endpoints.
            confluent.support.metrics.endpoint.secure=https://support-metrics.confluent.io/anon

            # Endpoint for insecure transmission of support metrics to Confluent.
            #
            # The insecure endpoint has lower priority than the secure endpoint.
            #
            # To disable sharing metrics over the insecure endpoint set this variable to
            # an empty value.
            #
            # Note: To disable the sharing of any metrics with Confluent you must disable both
            #       the secure and the insecure endpoints.
            confluent.support.metrics.endpoint.insecure=http://support-metrics.confluent.io/anon


By default metrics are collected every 24 hours and are stored in an internal Kafka topic as well as sent to Confluent over HTTP(S).
To disable sending the data to Confluent make sure the two endpoints for HTTPS (line 36) and HTTP (line 47) are commented out. In this mode of operation
metrics are still collected and stored in the internal Kafka topic for your own inspection but never sent to Confluent. To completely
disable metric collection, comment out the Kafka topic (line 22).


Enhanced data collection
----------
For licensed customers, Confluent collects additional metrics that help proactively identify issues in the field.

What metrics are collected?
~~~~~~~~~~~~~~~~~~~~~~
Each broker in the cluster collects the following metrics:

* **Basic configuration data** - This includes all the metrics described above.
* **Confluent customer Id**
* **OS and environment configuration** - This includes data on the operating system, Java runtime and other advanced environment properties.
* **Cluster-wide metrics** - This includes data on Zookeeper configuration, topic replication and consistency and other cluster-wide properties.


Configuration: enabling or disabling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The default broker configuration file that comes with the Confluent Platform enables basic anonymous data collection as described. To enable enhanced data collection and full Proactive Support change the customer id from "anonymous" to the customer ID provided by confluent as shown below in line 1. In addition configure the HTTP(S) endpoints as shown in lines 2 and 3:

.. sourcecode:: bash
    :linenos:

            confluent.support.customer.id=YourCustomerId
            ...
            confluent.support.metrics.endpoint.secure=https://support-metrics.confluent.io/submit
            confluent.support.metrics.endpoint.insecure=http://support-metrics.confluent.io/submit


Manually sending data to Confluent
----------
If your organization does not allow direct HTTPS or HTTP communication with an external service, you can still send data to Confluent manually using a script provided with the Confluent Platform called "support-metrics-bundle" as shown below:

.. sourcecode:: bash
    :linenos:

            # e.g., usage /usr/bin/support-metrics-bundle localhost:2181
            $ /usr/bin/support-metrics-bundle <zookeeperServer>

The script will collect any metrics sent to an internal Kafka topic and place them in a compressed file. You can then include this file as part of your support ticket.
