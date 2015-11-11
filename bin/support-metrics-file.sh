#!/bin/bash
#
# Copyright 2015 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

MYSELF=`basename $0`

###
### Configuration
###
TOPIC="__ConfluentSupportTopic"
TIMESTAMP=`date -u +"%Y%m%d-%H%M%S"`
FILEOUT="support-metrics-${TOPIC}.${TIMESTAMP}.bin"


###
### Main
###

print_help() {
  local script_name="$1"
  echo "Usage: $script_name --zookeeper <server:port> [--topic <Kafka support topic>] [--output <output file>]"
  echo
  echo "Creates a so-called 'support metrics bundle' file in the current directory."
  echo "This support metrics bundle contains metrics retrieved from the target Kafka cluster."
  echo
  echo "Parameters:"
  echo "-zookeeper   The ZooKeeper connection string to access the Kafka cluster from"
  echo "             which metrics support will be retrieved."
  echo "             Example: 'localhost:2181'"
  echo "--topic      The Kafka topic from which the support metrics will be retrieved."
  echo "             Default: '$TOPIC'"
  echo "--output     Output filename of the support metrics bundle."
  echo "             Default: '$FILEOUT'"
  echo "             Note that, when using the default value, the timestamp is dynamically"
  echo "             generated at each run of this tool."
  echo "--help       Print this help message."
  echo
  echo "Important notes:"
  echo "* Make sure that Zookeeper and Kafka are running while executing this tool."
}

if [ $# -eq 0 ]; then
  print_help $MYSELF
  exit 1
fi

while [ $# -gt 0 ]; do
  COMMAND=$1
  case $COMMAND in
    --zookeeper)
      ZOOKEEPER_SERVER=$2
      shift 2
      ;;
    -t|--topic)
      TOPIC=$2
      shift 2
      ;;
    -o|--output)
      FILEOUT=$2
      shift 2
      ;;
    -h|--help)
      shift 1
      print_help $MYSELF
      exit 1
      ;;
    *)
      echo "*** ERROR: Unknown parameter '$COMMAND'"
      echo
      print_help $MYSELF
      exit 2
      break
      ;;
  esac
done

if [ "x$KAFKA_HEAP_OPTS" = "x" ]; then
  export KAFKA_HEAP_OPTS="-Xmx512M"
fi

exec $(dirname $0)/kafka-run-class io.confluent.support.metrics.tools.KafkaMetricsToFile $ZOOKEEPER_SERVER $TOPIC $FILEOUT
if [ $? -eq 0 ]; then
  echo "Support metrics bundle created at $FILEOUT.  You may attach this file to your support tickets."
else
  echo "ERROR: Could not create support metrics bundle."
fi
