package com.dragon.transfer.core.handle.sink;

import com.dragon.transfer.common.element.Column;
import com.dragon.transfer.common.element.Record;
import com.dragon.transfer.common.exception.DragonTException;
import com.dragon.transfer.core.utils.DBUtilsErrorCode;
import org.apache.commons.lang3.tuple.Triple;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * @Title
 * @Author dragon
 * @Description
 * @Date 2023/11/30 17:14
 **/
public abstract class Writer {

    public abstract void startWriter();


    public static abstract class Job {

        protected Boolean emptyAsNull;

        protected String table;

        protected Connection conn;


        public Job(Boolean emptyAsNull, String table, Connection conn) {
            this.emptyAsNull = emptyAsNull;
            this.table = table;
            this.conn = conn;
        }

        public abstract void writer() throws SQLException;

        protected abstract void doBatchSql(Connection connection, List<Record> buffer) throws SQLException;

        protected abstract void doOneSql(Connection connection, List<Record> buffer) throws SQLException;


        protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement,
                                                                    int columnIndex,
                                                                    int columnSqltype,
                                                                    String typeName,
                                                                    Column column) throws SQLException {
            java.util.Date utilDate;
            switch (columnSqltype) {
                case Types.CHAR:
                case Types.NCHAR:
                case Types.CLOB:
                case Types.NCLOB:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    preparedStatement.setString(columnIndex + 1, column.asString());
                    break;
                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIGINT:
                case Types.NUMERIC:
                case Types.DECIMAL:
                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE:
                    String strValue = column.asString();
                    if (emptyAsNull && "".equals(strValue)) {
                        preparedStatement.setString(columnIndex + 1, null);
                    } else {
                        preparedStatement.setString(columnIndex + 1, strValue);
                    }
                    break;

                //tinyint is a little special in some database like mysql {boolean->tinyint(1)}
                case Types.TINYINT:
                    Long longValue = column.asLong();
                    if (null == longValue) {
                        preparedStatement.setString(columnIndex + 1, null);
                    } else {
                        preparedStatement.setString(columnIndex + 1, longValue.toString());
                    }
                    break;

                // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
                case Types.DATE:
                    if (typeName.equalsIgnoreCase("year")) {
                        if (column.asBigInteger() == null) {
                            preparedStatement.setString(columnIndex + 1, null);
                        } else {
                            preparedStatement.setInt(columnIndex + 1, column.asBigInteger().intValue());
                        }
                    } else {
                        java.sql.Date sqlDate = null;
                        try {
                            utilDate = column.asDate();
                        } catch (DragonTException e) {
                            throw new SQLException(String.format(
                                    "Date 类型转换错误：[%s]", column));
                        }

                        if (null != utilDate) {
                            sqlDate = new java.sql.Date(utilDate.getTime());
                        }
                        preparedStatement.setDate(columnIndex + 1, sqlDate);
                    }
                    break;

                case Types.TIME:
                    java.sql.Time sqlTime = null;
                    try {
                        utilDate = column.asDate();
                    } catch (DragonTException e) {
                        throw new SQLException(String.format(
                                "TIME 类型转换错误：[%s]", column));
                    }

                    if (null != utilDate) {
                        sqlTime = new java.sql.Time(utilDate.getTime());
                    }
                    preparedStatement.setTime(columnIndex + 1, sqlTime);
                    break;
                case Types.TIMESTAMP:
                    java.sql.Timestamp sqlTimestamp = null;
                    try {
                        utilDate = column.asDate();
                    } catch (DragonTException e) {
                        throw new SQLException(String.format(
                                "TIMESTAMP 类型转换错误：[%s]", column));
                    }
                    if (null != utilDate) {
                        sqlTimestamp = new java.sql.Timestamp(
                                utilDate.getTime());
                    }
                    preparedStatement.setTimestamp(columnIndex + 1, sqlTimestamp);
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.BLOB:
                case Types.LONGVARBINARY:
                    preparedStatement.setBytes(columnIndex + 1, column
                            .asBytes());
                    break;
                case Types.BOOLEAN:
                    preparedStatement.setBoolean(columnIndex + 1, column.asBoolean());
                    break;
                case Types.BIT:
                    preparedStatement.setBoolean(columnIndex + 1, column.asBoolean());
                    break;
                default:
                    throw DragonTException
                            .asException(DBUtilsErrorCode.WRITE_DATA_ERROR, "");
            }
            return preparedStatement;
        }

    }
}
