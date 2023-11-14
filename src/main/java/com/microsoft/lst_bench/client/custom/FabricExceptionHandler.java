package com.microsoft.lst_bench.client.custom;

import java.sql.SQLException;

import com.microsoft.lst_bench.client.ExceptionHandler;

public class FabricExceptionHandler implements ExceptionHandler  {

    private final String errorConflictMessage = "Snapshot isolation transaction aborted due to update conflict";

    @Override
    public Boolean isHandled(Exception e) {
        if (e instanceof SQLException) {
            SQLException sqlException = (SQLException)e;
            if (sqlException.getMessage().contains(errorConflictMessage)) {
                 return true;
            }
        }
        return false;
    }
}
