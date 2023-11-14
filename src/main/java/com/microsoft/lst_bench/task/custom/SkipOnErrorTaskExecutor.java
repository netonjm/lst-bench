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
package com.microsoft.lst_bench.task.custom;

import com.microsoft.lst_bench.client.ClientException;
import com.microsoft.lst_bench.client.Connection;
import com.microsoft.lst_bench.client.QueryResult;
import com.microsoft.lst_bench.exec.StatementExec;
import com.microsoft.lst_bench.task.TaskExecutor;
import com.microsoft.lst_bench.telemetry.EventInfo.Status;
import com.microsoft.lst_bench.telemetry.SQLTelemetryRegistry;
import com.microsoft.lst_bench.util.StringUtils;

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
public class SkipOnErrorTaskExecutor extends TaskExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(SkipOnErrorTaskExecutor.class);

  protected final String[] exceptionStrings;

  private final String SKIP_ERRONEOUS_QUERY_DELIMITER = ";";
  private final String SKIP_ERRONEOUS_QUERY_STRINGS_KEY = "skip_erroneous_query_strings";

  public SkipOnErrorTaskExecutor(
      SQLTelemetryRegistry telemetryRegistry,
      String experimentStartTime,
      Map<String, String> arguments) {
    super(telemetryRegistry, experimentStartTime, arguments);
    this.exceptionStrings = getExceptionStrings();
  }

  private String[] getExceptionStrings() {
    // Check whether there are any strings that errors are allowed to contain. In that case, we skip
    // the erroneous query and log a warning.
    String[] exceptionStrings;
    if (this.getArguments() == null
        || this.getArguments().get(SKIP_ERRONEOUS_QUERY_STRINGS_KEY) == null) {
      exceptionStrings = new String[] {};
    } else {
      exceptionStrings =
          this.getArguments()
              .get(SKIP_ERRONEOUS_QUERY_STRINGS_KEY)
              .split(SKIP_ERRONEOUS_QUERY_DELIMITER);
    }
    return exceptionStrings;
  }

  @Override
 protected final QueryResult executeStatement(
      Connection connection,
      StatementExec statement,
      Map<String, Object> values,
      boolean ignoreResults)
      throws ClientException {
    boolean skip = false;
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
              + "; error message: "
              + e.getMessage();
      for (String skipException : exceptionStrings) {
        if (e.getMessage().contains(skipException)) {
          LOGGER.warn(loggedError);
          writeStatementEvent(
              statementStartTime, statement.getId(), Status.WARN, /* payload= */ loggedError);

          skip = true;
          break;
        }
      }

      if (!skip) {
        LOGGER.error(loggedError);
        writeStatementEvent(
            statementStartTime, statement.getId(), Status.FAILURE, /* payload= */ loggedError);

        throw e;
      }
    }
    // Only log success if we have not skipped execution.
    if (!skip) {
      writeStatementEvent(
          statementStartTime, statement.getId(), Status.SUCCESS, /* payload= */ null);
    }
    return queryResult;
  } 
}
