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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

public class LogEndOffsetFetchResponse extends AbstractResponse {

  private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.LOG_END_OFFSET_FETCH.id);
  private static final String LOG_END_OFFSET_KEY_NAME = "log_end_offset";
  private static final String ERROR_CODE_KEY_NAME = "error_code";

  /**
   * Possible error codes:
   */

  private final long logEndOffset;
  private final short errorCode;

  public LogEndOffsetFetchResponse(long logEndOffset, Errors error) {
    super(new Struct(CURRENT_SCHEMA));
    struct.set(LOG_END_OFFSET_KEY_NAME, logEndOffset);
    struct.set(ERROR_CODE_KEY_NAME, error.code());

    this.logEndOffset = logEndOffset;
    this.errorCode = error.code();
  }

  public LogEndOffsetFetchResponse(Struct struct) {
    super(struct);
    logEndOffset = struct.getLong(LOG_END_OFFSET_KEY_NAME);
    errorCode = struct.getShort(ERROR_CODE_KEY_NAME);
  }

  public long logEndOffset() {
    return logEndOffset;
  }

  public short errorCode() {
    return errorCode;
  }

  public static LogEndOffsetFetchResponse parse(ByteBuffer buffer) {
    return new LogEndOffsetFetchResponse(CURRENT_SCHEMA.read(buffer));
  }
}
