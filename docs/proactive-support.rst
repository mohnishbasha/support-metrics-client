.. _ps_intro:

Confluent Proactive Support
===========================

**This document is a high-level summary.  Please refer to the** `Confluent Privacy Policy <http://www.confluent.io/privacy>`_ **as the authoritative source of information.**


What is Proactive Support?
--------------------------

Proactive Support is a component of the Confluent Platform.  It contains a feature that collects and reports support metrics ("Metrics"), which is enabled by default.  We do this primarily to provide proactive support to our customers, to help us build better products, to help customers comply with support contracts, and to help guide our marketing efforts.  With Metrics enabled, a Kafka broker is configured to collect and report certain broker and cluster metadata ("Metadata") every 24 hours about your use of the Confluent Platform 2.0 (including without limitation, your remote internet protocol address) to Confluent, Inc. (“Confluent”) or its parent, subsidiaries, affiliates or service providers.  This Metadata may be transferred to any country in which Confluent maintains facilities.

By proceeding with Metrics enabled, you agree to all such collection, transfer, storage and use of Metadata by Confluent.  You can turn Metrics off at any time by following the instructions described below.

Please refer to the `Confluent Privacy Policy <http://www.confluent.io/privacy>`_ for an in-depth description of how Confluent processes such information.


.. _ps-how-it-works:

How it works
------------

With Metrics enabled, a Kafka broker will collect and report certain broker and cluster metadata every 24 hours to:

1. a Kafka topic within the same cluster and
2. to Confluent via either HTTPS or HTTP over the Internet (HTTPS preferred).

The following sections describe in more detail which metadata is being collected, how to enable or disable the Metrics feature, how to configure the feature if you are a licensed Confluent customer, and how to tune its configuration settings when needed.


Which Metadata is being collected?
----------------------------------

Anonymous metadata collection (default)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With Metrics enabled, the default Kafka broker configuration that ships with the Confluent Platform collects and reports the following pieces of information anonymously to Confluent:

* **Confluent Platform version** - The Confluent Platform version that the broker is running.
* **Kafka version** - The Kafka version that the broker is running.
* **Broker token** - A dynamically generated unique identifier that is valid for the runtime of a broker.  The token is generated at broker startup and lost at shutdown.
* **Timestamp** - Time when the metadata was collected on the broker.


Metadata collection for licensed Confluent Customers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With Metrics enabled and with a proper Confluent customer ID set in the broker configuration (which is not the case for the default broker configuration that ships with the Confluent Platform) via the setting ``confluent.support.customer.id``, then additional Metadata is being collected and reported to Confluent that e.g. help to proactively identify issues in the field.  Please reach out to our customer support or refer to the `Confluent Privacy Policy <http://www.confluent.io/privacy>`_ for more information.


Enabling or disabling the Metrics feature
-----------------------------------------

The Metrics feature can be enabled or disabled at any time by modifying the broker configuration as appropriate, followed by a restart of the broker.

The relevant broker configuration setting is described below:

.. sourcecode:: bash
    :linenos:

            ##################### Confluent Proactive Support ######################

            # If set to true, then the feature to collect and report support metrics
            # ("Metrics") is enabled.  If set to false, the feature is disabled.
            confluent.support.metrics.enable=true

By default metrics are collected every 24 hours and are stored in an internal Kafka topic as well as sent to Confluent over HTTP(S).
To disable sending the data to Confluent make sure the two endpoints for HTTPS (line 36) and HTTP (line 47) are commented out. In this mode of operation
metrics are still collected and stored in the internal Kafka topic for your own inspection but never sent to Confluent. To completely
disable metric collection, comment out the Kafka topic (line 22).


Recommended Proactive Support configuration settings for licensed Confluent customers
-------------------------------------------------------------------------------------

Confluent customers must change a few settings in the default broker configuration as described below to ensure that additional Metadata is being collected and reported to Confluent.  Notably, you must provide your Confluent customer ID and, for reporting support metrics to Confluent via the Internet, change the default endpoints to customer-only HTTPS/HTTP endpoints.

.. sourcecode:: bash
    :linenos:

            ##################### Confluent Proactive Support ######################

            # Recommended settings for licensed Confluent customers
            confluent.support.metrics.enable=true
            confluent.support.customer.id=REPLACE_WITH_YOUR_CUSTOMER_ID
            confluent.support.metrics.endpoint.secure=https://support-metrics.confluent.io/submit
            confluent.support.metrics.endpoint.insecure=http://support-metrics.confluent.io/submit


Available Proactive Support configuration settings
--------------------------------------------------

This section documents all available Proactive Support settings that can be defined in the broker configuration.  Most users will not need to change these settings.  In fact, we recommend to leave these settings at their default values;  the exception are Confluent customers, which should change a few settings as described in the previous section.

.. sourcecode:: bash
    :linenos:

            ##################### Confluent Proactive Support ######################

            # If set to true, then the feature to collect and report support metrics
            # ("Metrics") is enabled.  If set to false, the feature is disabled.
            confluent.support.metrics.enable=true

            # The customer ID under which support metrics will be collected and
            # reported.
            #
            # When the customer ID is set to "anonymous" (the default), then only
            # a reduced set of metrics is being collected and reported.
            #
            # If you are a Confluent customer, then you should replace the default
            # value with your actual Confluent customer ID.  Doing so will ensure
            # that additional support metrics will be collected and reported.
            confluent.support.customer.id=anonymous

            # The interval at which support metrics will be collected from and reported
            # by this broker.
            confluent.support.metrics.report.interval.hours=24

            # The Kafka topic (within the same cluster as this broker) to which support
            # metrics will be submitted.
            #
            # To specifically disable the collection of support metrics to an internal
            # Kafka topic set this variable to an empty value.  This setting gives you the
            # flexibility to continue to collect and report Metadata in general
            # (cf. confluent.support.metrics.enable) but selectively turn off the reporting
            # to an internal Kafka topic.
            confluent.support.metrics.topic=__confluent.support.metrics

            # Endpoint for secure reporting of support metrics to Confluent via the Internet.
            #
            # The secure endpoint takes precedence over the insecure endpoint, i.e. if the
            # secure endpoint is enabled, then metrics will not be reported via the insecure
            # endpoint.  If the secure endpoint is not reachable, metrics reporting falls back
            # to the insecure endpoint (if enabled).
            #
            # To disable reporting metrics over the secure endpoint set this variable to an
            # empty value.
            confluent.support.metrics.endpoint.secure=https://support-metrics.confluent.io/anon

            # Endpoint for insecure reporting of support metrics to Confluent via the Internet.
            #
            # The insecure endpoint has lower priority than the secure endpoint.
            #
            # To disable reporting metrics over the insecure endpoint set this variable to an
            # empty value.
            confluent.support.metrics.endpoint.insecure=http://support-metrics.confluent.io/anon



Sharing Proactive Support Metadata with Confluent manually
----------------------------------------------------------

There are certain situations when reporting the Metadata via the Internet is not possible.  For example, a company's security policy may mandate that computer infrastructure in production environments must not be able to access the Internet directly.  This is the main reason why the Metrics feature includes the functionality to report the collected metadata to an internal Kafka topic (see section :ref:`ps-how-it-works`).

For these situations we ship a utility with the Confluent Platform as part of the Kafka installation package that will retrieve any previously reported metadata from the internal Kafka topic and store them in a compressed file.  You can then share this file with our customer support, e.g. by attaching it to a support ticket.


.. sourcecode:: bash

    # The `support-metrics-bundle` bequires the Kafka package of
    $ foo

asdf

.. sourcecode:: bash

    # The `support-metrics-bundle` bequires the Kafka package of
    # Confluent Platform being installed.

    # Example:
    # --------
    # Here we connect to the Kafka cluster backed by the ZooKeeper
    # ensemble reachable at `zookeeper1:2181`.  Retrieved metadata
    # will be stored in a local file (the tool will inform you about
    # the name and location of the file at the end of its run).
    $ /usr/bin/support-metrics-bundle --zookeeper zookeeper1:2181

    # Usage
    # -----
    $ /usr/bin/support-metrics-bundle --help
    Usage: support-metrics-bundle --zookeeper <server:port> [--topic <Kafka support topic>] [--file <bundle output file>] [--runtime <time in seconds>]

    Creates a so-called 'support metrics bundle' file in the current directory.
    This support metrics bundle contains metrics retrieved from the target Kafka cluster.


    Parameters:
    --zookeeper  The ZooKeeper connection string to access the Kafka cluster from
                 which metrics support will be retrieved.
                 Example: 'localhost:2181'
    --topic      The Kafka topic from which the support metrics will be retrieved.
                 Default: '__confluent.support.metrics'
    --file       Output filename of the support metrics bundle.
                 Default: 'support-metrics-__confluent.support.metrics.20151203-115035.zip'
                 Note that, when using the default value, the timestamp is dynamically
                 generated at each run of this tool.
    --runtime    The time this script will run for in seconds. For a large cluster
                 the script might need more time to collect all records.
                 Default: '10' seconds

.. sourcecode:: bash

    --help       Print this help message.


    Important notes for running this tool:
    * Kafka and ZooKeeper must be up and running.
    * Kafka and Zookeeper must be accessible from the machine on which this tool is executed.

    Copyright 2015 Confluent Inc. <http://confluent.io/>

Should you have any questions about the usage of this tool, then please contact our customer support.
