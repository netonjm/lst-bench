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
package com.microsoft.lst_bench.client;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A JDBC connection. */
public class JDBCConnection implements Connection {

  private static final Logger LOGGER = LoggerFactory.getLogger(JDBCConnection.class);

  private final java.sql.Connection connection;
  private final int maxNumRetries;
  private final ExceptionHandler exceptionHandler;

  public JDBCConnection(java.sql.Connection connection, int maxNumRetries, ExceptionHandler exceptionHandler) {
    this.connection = connection;
    this.maxNumRetries = maxNumRetries;
    this.exceptionHandler = exceptionHandler;
  }

  @Override
  public void execute(String sqlText) throws ClientException {
    execute(sqlText, true);
  }

  @Override
  public QueryResult executeQuery(String sqlText) throws ClientException {
    return execute(sqlText, false);
  }

  private QueryResult execute(String sqlText, boolean ignoreResults) throws ClientException {
    QueryResult queryResult = null;
    int errorCount = 0;

    // Retry count is in addition to the 1 default try, thus '<='.
    while (errorCount <= this.maxNumRetries) {
      try (Statement s = connection.createStatement()) {
        boolean hasResults = s.execute(sqlText);
        if (hasResults) {
          ResultSet rs = s.getResultSet();
          if (ignoreResults) {
            while (rs.next()) {
              // do nothing
            }
          } else {
            queryResult = new QueryResult();
            queryResult.populate(rs);
          }
        }
        // Return here if successful
        return queryResult;
      } catch (Exception e) {
        queryResult = null;
        String lastErrorMsg =
            "Query execution ("
                + this.maxNumRetries
                + " retries) unsuccessful; stack trace: "
                + ExceptionUtils.getStackTrace(e);
        if (errorCount == this.maxNumRetries) {
          LOGGER.error(lastErrorMsg);
          throw new ClientException(lastErrorMsg);
        } else {
          LOGGER.warn(lastErrorMsg);
        }
        errorCount++;
      }
    }
    // Return here if max retries reached without success
    return queryResult;
  }

  @Override
  public void close() throws ClientException {
    try {
      connection.close();
    } catch (SQLException e) {
      throw new ClientException(e);
    }
  }

  @Override
  public String getExceptionMessage(Exception e) 
  {
    if (e instanceof SQLException) {
      SQLException sqlException = (SQLException)e;
      return "; SQL State: "
        + sqlException.getSQLState()
        + "; Error Code: "
        + sqlException.getErrorCode();
    }
    return null;
  }

  @Override
  public Boolean isExceptionHandled(Exception exception)
  {
    if (this.exceptionHandler != null && this.exceptionHandler.isHandled(exception)) {
      return true;
    }
    return false;
  }
}
