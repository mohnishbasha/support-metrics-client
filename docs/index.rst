.. Confluent Proactive Support documentation master file

Confluent Proactive Support
===========================

.. _ps_intro:

What is Proactive Support?
--------------------------

Proactive Support is a component of the Confluent Platform.  It collects and reports support metrics ("Metrics") to Confluent. Proactive Support is enabled by default in the Confluent Platform.  We do this to provide proactive support to our customers, to help us build better products, to help customers comply with support contracts, and to help guide our marketing efforts.  With Metrics enabled, a Kafka broker is configured to collect and report certain broker and cluster metadata ("Metadata") every 24 hours about your use of the Confluent Platform (including without limitation, your remote internet protocol address) to Confluent, Inc. (“Confluent”) or its parent, subsidiaries, affiliates or service providers.  This Metadata may be transferred to any country in which Confluent maintains facilities.

By proceeding with Metrics enabled, you agree to all such collection, transfer, storage and use of Metadata by Confluent.  You can turn Metrics off at any time by following the instructions described below.

Please refer to the `Confluent Privacy Policy <http://www.confluent.io/privacy>`_ for an in-depth description of how Confluent processes such information.


.. _ps-how-it-works:

How it works
------------

With the Metrics feature enabled, a Kafka broker will collect and report certain broker and cluster metadata every 24 hours to the following two destinations:

1. to a special-purpose Kafka topic within the same cluster (named ``__confluent.support.metrics`` by default)
2. to Confluent via either HTTPS or HTTP over the Internet (HTTPS preferred).

The main reason for reporting to a Kafka topic is that there are certain situations when reporting the metadata via the Internet is not possible.  For example, a company's security policy may mandate that computer infrastructure in production environments must not be able to access the Internet directly.  The drawback of this approach is that the collected metadata is not being shared automatically and requires manual operator intervention as described in section :ref:`ps-sharing-metadata-manually`.  Reporting data to Confluent via the Internet is the most convenient option for most customers.

The agent that collects and reports the metadata is collocated with the broker process and runs within the same JVM.  The volume of the metadata collected by this agent (see :ref:`ps-which-metadata-is-being-collected`) is small and the default report interval is once every 24 hours (see :ref:`ps-configuration-settings`).

The following sections describe in more detail which metadata is being collected, how to enable or disable the Metrics feature, how to configure the feature if you are a licensed Confluent customer, and how to tune its configuration settings when needed.

.. _ps-which-metadata-is-being-collected:

Which metadata is collected?
-----------------------------

Proactive Support has two versions, one for open-source users (called Version Collector) and another for licenced Confluent Customers (called Confluent Support Metrics). The former is included in Confluent Kafka package while the latter needs to be installed as a separate package called Confluent Support Metrics. If you installed Confluent Platform package (and not just Confluent Platform Open Source), the full Confluent Support Metrics collector is included.

Version Collector (default)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Version Collector package collects the Kafka and Confluent Platform versions and
reports the following pieces of information to Confluent:

* **Confluent Platform version** - The Confluent Platform version that the broker is running.
* **Kafka version** - The Kafka version that the broker is running.
* **Broker token** - A dynamically generated unique identifier that is valid for the runtime of a broker.  The token is generated at broker startup and lost at shutdown.
* **Timestamp** - Time when the metadata was collected on the broker.


Confluent Support Metrics (add-on package)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Confluent Support Metrics add-on package collects and reports additional metadata that helps Confluent to proactively identify issues in the field. This additional metadata includes but is not limited to information about the Java runtime environment of the Kafka broker and metrics relevant to help to comply with Confluent support contracts.

Please reach out to our customer support or refer to the `Confluent Privacy Policy <http://www.confluent.io/privacy>`_ for more information. You will need a Confluent customer ID to set in the broker configuration (``confluent.support.customer.id`` setting) as well as the Confluent Support Metrics package.


Which metadata and data is not being collected?
------------------------------------------------

We understand that Confluent Platform users often publish private or proprietary data to Kafka clusters, and often run Kafka in sensitive environments.  We have done our best to avoid collecting any proprietary information about customers.  In particular:

* We do not inspect any messages sent through Kafka, and collect no data about what is inside these messages.
* We do not collect any proprietary network information such as internal host names or IP addresses.
* We do not collect information about the name of topics.

Installing Support Metrics
---------------------------

In order to collect and report the additional support metadata, you need to have the Support Metrics package installed.

If you installed confluent-3.1.1-2.11.8 or confluent-3.1.1-2.10.6 (through ZIP, TAR, DEB or RPM), then Support Metrics is already installed and the next step is to obtain your Confluent customer ID from our customer support and to update it in Kafka configuration (see :ref:`ps-customer-id-configuration`).

If you installed confluent-oss-3.1.1-2.11.8 or confluent-oss-3.1.1-2.10.6, or if you chose to install individual packages, you will need to install confluent-support-metrics_3.1.1 packages first.

DEB Packages via apt
~~~~~~~~~~~~~~~~~~~~~

We'll assume you already followed instructions here :ref:`installation_apt` to install Confluent's public key and add the repository.

Run apt-get update and install Support Metrics package:

.. sourcecode:: bash

      $ sudo apt-get update && sudo apt-get install confluent-support-metrics_3.1.1

The next step is to obtain your Confluent customer ID from our customer support and to update it in Kafka configuration (see :ref:`ps-customer-id-configuration`).

RPM Packages via yum
~~~~~~~~~~~~~~~~~~~~~

We'll assume you already followed instructions here :ref:`installation_yum` to install Confluent's public key and add the repository.

It is recommended to `clear the yum caches <https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Deployment_Guide/sec-Working_with_Yum_Cache.html>`_ before proceeding:

.. sourcecode:: bash

    $ sudo yum clean all

The repository is now ready for use.

You can install Support Metrics with:

.. sourcecode:: bash

    $ sudo yum install confluent-platform-2.11.8

The next step is to obtain your Confluent customer ID from our customer support and to update it in Kafka configuration (see :ref:`ps-customer-id-configuration`).

Enabling or disabling the Metrics feature
-----------------------------------------

The Metrics feature can be enabled or disabled at any time by modifying the broker configuration as needed, followed by a restart of the broker.

The relevant setting for the broker configuration (typically at ``/etc/kafka/server.properties``) is described below:

.. sourcecode:: bash
    :linenos:

       ##################### Confluent Proactive Support:  ######################
       ##################### broker configuration settings ######################

       # If set to true, then the feature to collect and report support metrics
       # ("Metrics") is enabled.  If set to false, the feature is disabled.
       #
       # Note: If the feature is disabled, then the agent that is collocated with
       # the broker process and that collects and reports the support metrics
       # will also not be started.
       confluent.support.metrics.enable=true

.. _ps-customer-id-configuration:

Recommended Proactive Support configuration settings for licensed Confluent customers
-------------------------------------------------------------------------------------

Confluent customers must change the ``confluent.support.customer.id`` setting and provide their respective Confluent customer ID.  Please reach out to our customer support if you have any questions.

.. sourcecode:: bash
    :linenos:

       ##################### Confluent Proactive Support:  ######################
       ##################### broker configuration settings ######################

       # Recommended settings for licensed Confluent customers
       confluent.support.metrics.enable=true
       confluent.support.customer.id=REPLACE_WITH_YOUR_CUSTOMER_ID


.. _ps-configuration-settings:

Proactive Support configuration settings
----------------------------------------

This section documents all available Proactive Support settings that can be defined in the broker configuration (typically at ``/etc/kafka/server.properties``), including their default values.  Most users will not need to change these settings.  In fact, we recommend leaving these settings at their default values;  the exception are Confluent customers, which should change a few settings as described in the previous section.

.. sourcecode:: bash

    ##################### Confluent Proactive Support:  ######################
    ##################### broker configuration settings ######################

    # If set to true, then the feature to collect and report support metrics
    # ("Metrics") is enabled.  If set to false, the feature is disabled.
    #
    confluent.support.metrics.enable=true

    # The customer ID under which support metrics will be collected and
    # reported.
    #
    # When the customer ID is set to "anonymous" (the default), then only a
    # reduced set of metrics is being collected and reported.
    #
    # Confluent customers
    # -------------------
    # If you are a Confluent customer, then you should replace the default
    # value with your actual Confluent customer ID.  Doing so will ensure
    # that additional support metrics will be collected and reported.
    #
    confluent.support.customer.id=anonymous

    # The Kafka topic (within the same cluster as this broker) to which support
    # metrics will be submitted.
    #
    # To specifically disable reporting metrics to an internal Kafka topic when
    # `confluent.support.metrics.enable=true` set this variable to an empty value.
    #
    confluent.support.metrics.topic=__confluent.support.metrics

    # The interval at which support metrics will be collected from and reported
    # by this broker.
    #
    confluent.support.metrics.report.interval.hours=24

    # To selectively disable the reporting of support metrics to Confluent
    # over the Internet when `confluent.support.metrics.enable=true`,
    # set these variables to false as needed.
    #
    # Tip: If you want to enforce that reporting over the Internet
    # will only ever use an encrypted channel, enable the secure
    # endpoint but disable the insecure one.
    #
    confluent.support.metrics.endpoint.insecure.enable=true
    confluent.support.metrics.endpoint.secure.enable=true


Network ports used by Proactive Support
---------------------------------------

When the Metrics feature is enabled (default), brokers will attempt to report metadata via the Internet to Confluent.
The metadata will be sent via HTTPS (preferred) or HTTP, which means you need to ensure that the brokers are allowed
to talk to the Internet via destination ports `443` (HTTPS) and/or `80` (HTTP) if you want to benefit from this functionality.


.. _ps-sharing-metadata-manually:

Sharing Proactive Support Metadata with Confluent manually
----------------------------------------------------------

There are certain situations when reporting the metadata via the Internet is not possible.  For example, a company's security policy may mandate that computer infrastructure in production environments must not be able to access the Internet directly.  This is the main reason why the Metrics feature includes the functionality to report the collected metadata to an internal Kafka topic (see section :ref:`ps-how-it-works`).

For these situations we include a tool called ``support-metrics-bundle`` in the Kafka installation package of the Confluent Platform that will retrieve any previously reported metadata from the internal Kafka topic and store them in a compressed file.  You can then share this file with our customer support, e.g. by attaching it to a support ticket.

.. sourcecode:: bash

    ###
    ### IMPORTANT: The `support-metrics-bundle` tool requires that the Kafka package of
    ###            Confluent Platform is installed.
    ###

    # Example
    # -------
    # Here we connect to the Kafka cluster backed by the ZooKeeper
    # ensemble reachable at `zookeeper1:2181`.  Retrieved metadata
    # will be stored in a local file (the tool will inform you about
    # the name and location of the file at the end of its run).
    #
    $ /usr/bin/support-metrics-bundle --zookeeper zookeeper1:2181

    # Usage
    # -----
    #
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
    --runtime    The time in seconds this tool will run for.  For a large cluster
                 you may need to increase this setting because the tool might need
                 more time to collect all the metrics.
                 Default: 10
    --help       Print this help message.


    Important notes for running this tool:
    * Kafka and ZooKeeper must be up and running.
    * Kafka and Zookeeper must be accessible from the machine on which this tool is executed.

Should you have any questions about the usage of this tool, then please contact Confluent customer support.

.. _ps_privacy:

Privacy Policy
--------------

Please refer to the `Confluent Privacy Policy <http://www.confluent.io/privacy>`_.

