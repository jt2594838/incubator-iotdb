/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.exception;

/**
 * This Exception is the parent class for all query engine runtime exceptions.<br> This Exception
 * extends super class {@link java.lang.RuntimeException}
 *
 * @author CGF
 */
public abstract class QueryEngineRunningException extends RuntimeException {

  private static final long serialVersionUID = 7537799061005397794L;

  public QueryEngineRunningException() {
    super();
  }

  public QueryEngineRunningException(String message, Throwable cause) {
    super(message, cause);
  }

  public QueryEngineRunningException(String message) {
    super(message);
  }

  public QueryEngineRunningException(Throwable cause) {
    super(cause);
  }

}
