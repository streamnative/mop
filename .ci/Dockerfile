#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

ARG PULSAR_FQ_IMAGE
FROM ${PULSAR_FQ_IMAGE}

USER root
RUN rm -rf /pulsar/protocols/pulsar-protocol-handler-mqtt-*.nar

COPY ../mqtt-impl/target/pulsar-protocol-handler-mqtt-${PULSAR_FQ_IMAGE}-.nar /pulsar/protocols