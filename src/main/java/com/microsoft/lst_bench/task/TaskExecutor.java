/*
 * Copyright (c) Microsoft Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.lst_bench.task;

import com.microsoft.lst_bench.client.ClientException;
import com.microsoft.lst_bench.client.Connection;
import com.microsoft.lst_bench.client.QueryResult;
import com.microsoft.lst_bench.exec.FileExec;
import com.microsoft.lst_bench.exec.StatementExec;
import com.microsoft.lst_bench.exec.TaskExec;
import com.microsoft.lst_bench.telemetry.EventInfo;
import com.microsoft.lst_bench.telemetry.EventInfo.EventType;
import com.microsoft.lst_bench.telemetry.EventInfo.Status;
import com.microsoft.lst_bench.telemetry.ImmutableEventInfo;
import com.microsoft.lst_bench.telemetry.SQLTelemetryRegistry;
import com.microsoft.lst_bench.util.StringUtils;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default executor for tasks. Iterates over all files and all the statements contained in those
 * files and executes them sequentially. This task executor allows users to avoid failing execution
 * by skipping queries which return a specific substring in their error message. For this task
 * executor, we allow users to determine the exception strings as part of the input, by specifying
 * parameter 'skip_failed_query_task_strings'. If multiple strings can lead to a skip action, they
 * need to be separated with delimiter ';' by default.
 */
public class TaskExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(TaskExecutor.class);

  protected final SQLTelemetryRegistry telemetryRegistry;
  protected final String experimentStartTime;
  protected final Map<String, String> arguments;

  public TaskExecutor(
      SQLTelemetryRegistry telemetryRegistry,
      String experimentStartTime,
      Map<String, String> arguments) {
    this.experimentStartTime = experimentStartTime;
    this.telemetryRegistry = telemetryRegistry;
    this.arguments = arguments;
   
  }

  protected Map<String, String> getArguments() {
    return this.arguments;
  }

  public void executeTask(Connection connection, TaskExec task, Map<String, Object> values)
      throws ClientException {
    for (FileExec file : task.getFiles()) {
      Instant fileStartTime = Instant.now();
      try {
        for (StatementExec statement : file.getStatements()) {
          executeStatement(connection, statement, values, true);
        }
      } catch (Exception e) {
        LOGGER.error("Exception executing file: " + file.getId());
        writeFileEvent(fileStartTime, file.getId(), Status.FAILURE);
        throw e;
      }
      writeFileEvent(fileStartTime, file.getId(), Status.SUCCESS);
    }
  }

  protected QueryResult executeStatement(
      Connection connection,
      StatementExec statement,
      Map<String, Object> values,
      boolean ignoreResults)
      throws ClientException {
    QueryResult queryResult = null;
    Instant statementStartTime = Instant.now();
    try {
      if (ignoreResults) {
        connection.execute(StringUtils.replaceParameters(statement, values).getStatement());
      } else {
        queryResult =
            connection.executeQuery(
                StringUtils.replaceParameters(statement, values).getStatement());
      }
    } catch (Exception e) {
      
        String loggedError =
            "Exception executing statement: "
                + statement.getId()
                + ", statement text: "
                + statement.getStatement()
                + "; Error message: " + e.getMessage();

        LOGGER.error(loggedError);
        writeStatementEvent(
            statementStartTime, statement.getId(), Status.FAILURE, /* payload= */ loggedError);
        
        throw e;
    }
    // Only log success if we have not skipped execution.
    writeStatementEvent(
          statementStartTime, statement.getId(), Status.SUCCESS, /* payload= */ null);
    return queryResult;
  }

  protected final EventInfo writeFileEvent(Instant startTime, String id, Status status) {
    EventInfo eventInfo =
        ImmutableEventInfo.of(
            experimentStartTime, startTime, Instant.now(), id, EventType.EXEC_FILE, status);
    telemetryRegistry.writeEvent(eventInfo);
    return eventInfo;
  }

  protected final EventInfo writeStatementEvent(
      Instant startTime, String id, Status status, String payload) {
    EventInfo eventInfo = null;
    if (payload != null) {
      eventInfo =
          ImmutableEventInfo.of(
                  experimentStartTime,
                  startTime,
                  Instant.now(),
                  id,
                  EventType.EXEC_STATEMENT,
                  status)
              .withPayload(payload);
    } else {
      eventInfo =
          ImmutableEventInfo.of(
              experimentStartTime, startTime, Instant.now(), id, EventType.EXEC_STATEMENT, status);
    }
    telemetryRegistry.writeEvent(eventInfo);
    return eventInfo;
  }
}
