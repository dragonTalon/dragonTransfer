package com.dragon.transfer.core.handle.schema;

import com.dragon.transfer.common.exception.InvalidDbException;
import com.dragon.transfer.common.exception.code.DBErrorCode;
import com.dragon.transfer.core.utils.DBUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @Title
 * @Author dragon
 * @Description
 * @Date 2023/11/28 10:27
 **/
public class SchemaManager {

    private final static String table_desc = "desc %s";

    public static List<String> getDbTalesInfo(Connection conn)
            throws SQLException {

        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(1000);
        stmt.setQueryTimeout(30);
        ResultSet tablesRs = DBUtils.query(stmt, "SHOW TABLES");
        List<String> tableList = new ArrayList<>();
        while (tablesRs.next()) {
            tableList.add(tablesRs.getString(1));
        }
        if (tableList.isEmpty()) {
            throw new InvalidDbException(DBErrorCode.DB_TABLE_EMPTY);
        }

        return tableList;
    }


    private static FieldSchema buildFieldInfo(ResultSet rs) throws SQLException {
        FieldSchema tableMeta = new FieldSchema();
        tableMeta.setName(rs.getString("Field"));
        tableMeta.setType(rs.getString("Type"));

        String key = rs.getString("Key");
        if (StringUtils.isNotBlank(key) && StringUtils.equals(key, "PRI")) {
            tableMeta.setPri(Boolean.TRUE);
        }
        return tableMeta;
    }

}
