/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Struct;

public class LogEndOffsetFetchRequest extends AbstractRequest {

  private static final String CONTROLLER_ID_KEY_NAME = "controller_id";
  private static final String CONTROLLER_EPOCH_KEY_NAME = "controller_epoch";
  private static final String TOPIC_KEY_NAME = "topic";
  private static final String PARTITION_KEY_NAME = "partition";

  public static class Builder extends AbstractRequest.Builder<LogEndOffsetFetchRequest> {
    private final int controllerId;
    private final int controllerEpoch;
    private final String topic;
    private final int partition;

    public Builder(int controllerId, int controllerEpoch, String topic, int partition) {
      super(ApiKeys.LOG_END_OFFSET_FETCH);
      this.controllerId = controllerId;
      this.controllerEpoch = controllerEpoch;
      this.topic = topic;
      this.partition = partition;
    }

    @Override
    public LogEndOffsetFetchRequest build() {
      return new LogEndOffsetFetchRequest(controllerId, controllerEpoch, topic, partition, version());
    }

    @Override
    public String toString() {
      StringBuilder bld = new StringBuilder();
      bld.append("(type: LogEndOffsetFetchRequest=").
          append(", controllerId=").append(controllerId).
          append(", controllerEpoch=").append(controllerEpoch).
          append(", topic=").append(topic).
          append(", partition=").append(partition).
          append(")");
      return bld.toString();
    }
  }

  private final int controllerId;
  private final int controllerEpoch;
  private final String topic;
  private final int partition;

  private LogEndOffsetFetchRequest(int controllerId, int controllerEpoch, String topic, int partition, short version) {
    super(new Struct(ProtoUtils.requestSchema(ApiKeys.LOG_END_OFFSET_FETCH.id, version)), version);

    struct.set(CONTROLLER_ID_KEY_NAME, controllerId);
    struct.set(CONTROLLER_EPOCH_KEY_NAME, controllerEpoch);
    struct.set(TOPIC_KEY_NAME, topic);
    struct.set(PARTITION_KEY_NAME, partition);

    this.controllerId = controllerId;
    this.controllerEpoch = controllerEpoch;
    this.topic = topic;
    this.partition = partition;
  }

  public LogEndOffsetFetchRequest(Struct struct, short versionId) {
    super(struct, versionId);
    controllerId = struct.getInt(CONTROLLER_ID_KEY_NAME);
    controllerEpoch = struct.getInt(CONTROLLER_EPOCH_KEY_NAME);
    topic = struct.getString(TOPIC_KEY_NAME);
    partition = struct.getInt(PARTITION_KEY_NAME);
  }

  public int controllerId() {
    return controllerId;
  }

  public int controllerEpoch() {
    return controllerEpoch;
  }

  public String topic() {
    return topic;
  }

  public int partition() {
    return partition;
  }

  @Override
  public AbstractResponse getErrorResponse(Throwable e) {
    return null;
  }

  public static LogEndOffsetFetchRequest parse(ByteBuffer buffer, int versionId) {
    return new LogEndOffsetFetchRequest(ProtoUtils.parseRequest(ApiKeys.LOG_END_OFFSET_FETCH.id, versionId, buffer), (short) versionId);
  }

  public static LogEndOffsetFetchRequest parse(ByteBuffer buffer) {
    return parse(buffer, ProtoUtils.latestVersion(ApiKeys.LOG_END_OFFSET_FETCH.id));
  }
}
