package com.dragon.transfer.core.handle.sink.mysql;

import com.dragon.transfer.common.element.Column;
import com.dragon.transfer.common.element.Record;
import com.dragon.transfer.common.exception.DataOperationException;
import com.dragon.transfer.common.exception.DragonTException;
import com.dragon.transfer.common.exception.code.CommonErrorCode;
import com.dragon.transfer.common.exception.code.DBErrorCode;
import com.dragon.transfer.common.exception.code.TableErrorCode;
import com.dragon.transfer.core.handle.record.StructRecord;
import com.dragon.transfer.core.handle.sink.Writer;
import com.dragon.transfer.core.handle.transport.LocalTransport;
import com.dragon.transfer.core.utils.DBUtils;
import com.dragon.transfer.core.utils.DBUtilsErrorCode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

/**
 * @Title
 * @Author dragon
 * @Description
 * @Date 2023/11/29 17:11
 **/
public class MysqlStockWriter {

    private static Logger log = LoggerFactory.getLogger(MysqlStockWriter.class);

    public static final String DROP_SQL = "drop table IF EXISTS %s";

    private static final String VALUE_HOLDER = "?";

    private Boolean emptyAsNull = Boolean.TRUE;

    private Integer batchSize = 10;

    private LocalTransport transport;

    private DataSource dataSource;

    private String db;

    private Map<String/*table*/, String/*insert sql*/> sqlMap = new HashMap<>();

    private Map<String, Triple<List<String>, List<Integer>, List<String>>> tableMetaMap = new HashMap<>();

    public MysqlStockWriter(DataSource dataSource, LocalTransport transport, String db) {
        this.transport = transport;
        this.dataSource = dataSource;
        this.db = db;
    }

    public void startWriter() {
        try {
            Record record;
            List<Record> writeBuffer = new ArrayList<Record>(this.batchSize);
            while ((record = transport.get()) != null) {
                writeBuffer.add(record);
                if (writeBuffer.size() >= batchSize) {
                    writerWithConnection(dataSource.getConnection(), writeBuffer);
                    writeBuffer.clear();
                }
            }
            if (!writeBuffer.isEmpty()) {
                writerWithConnection(dataSource.getConnection(), writeBuffer);
            }
        } catch (SQLException e) {
            throw DragonTException.asException(DBErrorCode.DB_WRITE_FAIL, "写入数据失败" + db, e);
        }
    }


    /**
     * 写数据
     *
     * @param buffer
     */
    public void writerWithConnection(Connection conn, List<Record> buffer) {
        if (Objects.isNull(buffer) || buffer.size() == 0) {
            return;
        }
        Map<String, List<Record>> spilt = new HashMap<>(16);

        for (Record record : buffer) {
            switch (record.getType()) {
                case STRUCT:
                    try {
                        StructRecord structRecord = (StructRecord) record;
                        conn.setAutoCommit(false);
                        try (Statement stat = conn.createStatement();) {
                            if (structRecord.getForce()) {
                                stat.addBatch(String.format(DROP_SQL, structRecord.getTbName()));
                            }
                            Column column = structRecord.getColumn(0);
                            stat.addBatch(column.asString());
                            stat.executeBatch();
                            conn.commit();
                        } catch (SQLException e) {
                            if (structRecord.getForce()) {
                                throw DragonTException.asException(TableErrorCode.TABLE_CREATE_FAIL, "表创建失败" + record.getTbName(), e);
                            }
                            //非强制替换，默认是表存在
                            log.warn("表创建失败 {}", record.getTbName());
                        }
                    } catch (SQLException e) {
                        throw DragonTException.asException(DBErrorCode.DB_CONN_FAIL, "表创建失败" + record.getTbName(), e);
                    }
                    break;
                case ONLY_INSERT:
                    if (spilt.containsKey(record.getTbName())) {
                        spilt.get(record.getTbName()).add(record);
                    } else {
                        List<Record> records = new ArrayList<>();
                        records.add(record);
                        spilt.put(record.getTbName(), records);
                    }
                    break;
                case DONE:
                    log.info("db {} table {} 同步完成", db, record.getTbName());
                    break;
                default:
                    throw DragonTException.asException(CommonErrorCode.ILLEGAL_OPT, "存量操作不支持 DML DDL操作");
            }
            if (!tableMetaMap.containsKey(record.getTbName())) {
                tableMetaMap.put(record.getTbName(), DBUtils.getColumnMetaData(conn, record.getTbName(), null));
            }
            if (!sqlMap.containsKey(record.getTbName())) {
                sqlMap.put(record.getTbName(), calcWriteRecordSql(this.tableMetaMap.get(record.getTbName()), record.getTbName()));
            }
        }

        for (Map.Entry<String, List<Record>> entry : spilt.entrySet()) {
            try {
                String table = entry.getKey();
                MysqlWriterJob job = new MysqlWriterJob(emptyAsNull, table, dataSource.getConnection(), tableMetaMap.get(table), sqlMap.get(table), entry.getValue());
                job.writer();
            } catch (SQLException e) {
                throw DataOperationException.asException(DBUtilsErrorCode.WRITE_DATA_ERROR, e);
            }
        }
    }

    protected String calcWriteRecordSql(Triple<List<String>, List<Integer>, List<String>> triple, String table) {
        int columnSize = triple.getLeft().size();
        List<String> valueHolders = new ArrayList<String>(columnSize);
        List<String> columnHolders = new ArrayList<>(columnSize);
        for (int i = 0; i < columnSize; i++) {
            String type = triple.getRight().get(i);
            String columnName = triple.getLeft().get(i);
            valueHolders.add(calcValueHolder(type));
            columnHolders.add(columnName);
        }

        return new StringBuilder().append("INSERT")
                .append(" INTO ").append(table + "(").append(StringUtils.join(columnHolders, ","))
                .append(") VALUES(").append(StringUtils.join(valueHolders, ","))
                .append(")").toString();
    }


    protected String calcValueHolder(String columnType) {
        return VALUE_HOLDER;
    }


    class MysqlWriterJob extends Writer.Job {

        private Triple<List<String>, List<Integer>, List<String>> triple;

        private String writeRecordSql;

        private List<Record> buffer;

        public MysqlWriterJob(Boolean emptyAsNull, String table, Connection conn, Triple<List<String>, List<Integer>, List<String>> triple, String writeRecordSql, List<Record> buffer) {
            super(emptyAsNull, table, conn);
            this.triple = triple;
            this.writeRecordSql = writeRecordSql;
            this.buffer = buffer;
        }


        @Override
        public void writer() throws SQLException {
            doBatchSql(conn, buffer);
        }


        @Override
        protected void doBatchSql(Connection connection, List<Record> buffer)
                throws SQLException {
            if (Objects.isNull(buffer) || buffer.size() == 0) {
                return;
            }
            PreparedStatement preparedStatement = null;
            try {
                connection.setAutoCommit(false);
                preparedStatement = connection
                        .prepareStatement(writeRecordSql);

                for (Record record : buffer) {
                    preparedStatement = fillPreparedStatement(
                            preparedStatement, record);
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                connection.commit();
            } catch (SQLException e) {
                log.warn("回滚此次写入, 采用每次写入一行方式提交. 因为:" + e.getMessage());
                connection.rollback();
                doOneSql(connection, buffer);
            } catch (Exception e) {
                throw DataOperationException.asException(
                        DBUtilsErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                DBUtils.closeDBResources(preparedStatement, null);
            }
        }

        @Override
        protected void doOneSql(Connection connection, List<Record> buffer) {
            PreparedStatement preparedStatement = null;
            try {
                connection.setAutoCommit(true);
                preparedStatement = connection.prepareStatement(writeRecordSql);

                for (Record record : buffer) {
                    try {
                        preparedStatement = fillPreparedStatement(preparedStatement, record);
                        preparedStatement.execute();
                    } catch (SQLException e) {
                        log.warn("Insert fatal error SqlState ={}, errorCode = {}, {}", e.getSQLState(), e.getErrorCode(), e);
                    } finally {
                        preparedStatement.clearParameters();
                    }
                }
            } catch (Exception e) {
                throw DataOperationException.asException(DBUtilsErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                DBUtils.closeDBResources(preparedStatement, null);
            }
        }


        private PreparedStatement fillPreparedStatement(PreparedStatement preparedStatement, Record record) throws SQLException {
            for (int i = 0; i < triple.getLeft().size(); i++) {
                int columnSqltype = triple.getMiddle().get(i);
                String typeName = triple.getRight().get(i);
                preparedStatement = fillPreparedStatementColumnType(preparedStatement, i, columnSqltype, typeName, record.getColumn(i));
            }
            return preparedStatement;
        }

    }


}
