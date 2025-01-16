#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -ex

echo "Releasing MOP"

version=${1#v}
if [[ "x$version" == "x" ]]; then
  echo "You need give a version number of the MOP"
  exit 1
fi

# Create a direcotry to save assets
ASSETS_DIR=release
mkdir $ASSETS_DIR

mvn clean install -DskipTests -Dmaven.wagon.http.retryHandler.count=3
mv mqtt-broker/target/pulsar-protocol-handler-mqtt-*.jar ./$ASSETS_DIR/pulsar-protocol-handler-mqtt-"${version}".nar
mv mqtt-common/target/pulsar-protocol-handler-mqtt-common-*.jar ./$ASSETS_DIR/
mv mqtt-proxy/target/pulsar-protocol-handler-mqtt-proxy-*.jar ./$ASSETS_DIR/
cp README.md ./$ASSETS_DIR/pulsar-protocol-handler-mqtt-readme.md
