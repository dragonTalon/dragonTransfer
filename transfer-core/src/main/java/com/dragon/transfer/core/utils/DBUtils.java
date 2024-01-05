package com.dragon.transfer.core.utils;

import com.dragon.transfer.common.exception.DragonTException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Title
 * @Author dragon
 * @Description
 * @Date 2023/11/20 10:03
 **/
public class DBUtils {
    /**
     * get binlog position
     *
     * @param con
     * @return
     * @throws SQLException
     */
    public static BinLogSchema getBinlogPos(Connection con)
            throws SQLException {
        Statement stmt = con.createStatement();
        ResultSet rs = query(stmt, "SHOW MASTER STATUS");

        while (rs.next()) {
            String file = rs.getString("File");
            String position = rs.getString("Position");
            return new BinLogSchema(file, position);
        }
        return null;
    }


    /**
     * only ready
     *
     * @param conn
     * @param sql
     * @param fetchSize
     * @param queryTimeout
     * @return
     * @throws SQLException
     */
    public static ResultSet query(Connection conn, String sql, int fetchSize, int queryTimeout)
            throws SQLException {
        // make sure autocommit is off
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(fetchSize);
        stmt.setQueryTimeout(queryTimeout);
        return query(stmt, sql);
    }

    /**
     * sql query
     *
     * @param stmt
     * @param sql
     * @return
     * @throws SQLException
     */
    public static ResultSet query(Statement stmt, String sql)
            throws SQLException {
        return stmt.executeQuery(sql);
    }

    public static void closeDBResources(Statement stmt, Connection conn) {
        closeDBResources(null, stmt, conn);
    }

    /**
     * @return Left:ColumnName Middle:ColumnType Right:ColumnTypeName
     */
    public static Triple<List<String>, List<Integer>, List<String>> getColumnMetaData(
            Connection conn, String tableName, String column) {
        Statement statement = null;
        ResultSet rs = null;

        Triple<List<String>, List<Integer>, List<String>> columnMetaData = new ImmutableTriple<>(
                new ArrayList<String>(), new ArrayList<Integer>(),
                new ArrayList<String>());
        try {
            statement = conn.createStatement();
            String queryColumnSql = "";
            if (StringUtils.isBlank(column)) {
                queryColumnSql = "select * from " + tableName
                        + " where 1=2";
            } else {
                queryColumnSql = "select " + column + " from " + tableName
                        + " where 1=2";
            }

            rs = statement.executeQuery(queryColumnSql);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {

                columnMetaData.getLeft().add(rsMetaData.getColumnName(i + 1));
                columnMetaData.getMiddle().add(rsMetaData.getColumnType(i + 1));
                columnMetaData.getRight().add(
                        rsMetaData.getColumnTypeName(i + 1));
            }
            return columnMetaData;
        } catch (SQLException e) {
            throw DragonTException.asException(DBUtilsErrorCode.GET_COLUMN_INFO_FAILED,
                    String.format("获取表:%s 的字段的元信息时失败. 请联系 DBA 核查该库、表信息.", tableName), e);
        } finally {
            DBUtils.closeDBResources(rs, statement, null);
        }
    }

    /**
     * close connect
     *
     * @param rs
     * @param stmt
     * @param conn
     */
    public static void closeDBResources(ResultSet rs, Statement stmt,
                                        Connection conn) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException unused) {
            }
        }

        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException unused) {
            }
        }

        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException unused) {
            }
        }
    }
}
