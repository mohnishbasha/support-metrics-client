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

#
# This script produces a dump file in the current directory
#


if [ "x$KAFKA_HEAP_OPTS" = "x" ]; then
  export KAFKA_HEAP_OPTS="-Xmx512M"
fi

echo "Make sure that zookeeper and brokers are running while executing this command"
if [ $# -lt 3 ];
then
  echo "USAGE: $0 --zookeeper <server:port> --topic <Kafka support topic> [--o <output file>]"
  exit 1
fi

TOPIC="__ConfluentSupportTopic"
TIMESTAMP=`date -u +"%Y%m%d-%H%M%S"`
FILEOUT="support-${TOPIC}.${TIMESTAMP}.bin"
while [ $# -gt 0 ]; do
  COMMAND=$1
  case $COMMAND in
    --zookeeper)
      ZOOKEEPER_SERVER=$2
      shift 2
      ;;
    --topic)
      TOPIC=$2
      shift 2
      ;;
    --o)
      FILEOUT=$2
      shift 2
      ;;
    *)
      break
      ;;
  esac
done



exec $(dirname $0)/kafka-run-class io.confluent.support.metrics.tools.KafkaMetricsToFile $ZOOKEEPER_SERVER $TOPIC $FILEOUT
echo "Data collection complete. Please attach output file $FILEOUT to support ticket"
