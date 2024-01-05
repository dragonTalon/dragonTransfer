package com.dragon.transfer.core.handle.recevie.mysql;

import com.dragon.transfer.common.element.*;
import com.dragon.transfer.common.exception.DragonTException;
import com.dragon.transfer.common.exception.InvalidTableException;
import com.dragon.transfer.common.exception.code.CommonErrorCode;
import com.dragon.transfer.common.exception.code.DBErrorCode;
import com.dragon.transfer.common.exception.code.TableErrorCode;
import com.dragon.transfer.core.handle.recevie.Reader;
import com.dragon.transfer.core.handle.record.DoneRecord;
import com.dragon.transfer.core.handle.record.EndRecord;
import com.dragon.transfer.core.handle.record.StockRecord;
import com.dragon.transfer.core.handle.record.StructRecord;
import com.dragon.transfer.core.handle.schema.SchemaManager;
import com.dragon.transfer.core.handle.transport.LocalTransport;
import com.dragon.transfer.core.utils.Constant;
import com.dragon.transfer.core.utils.DBUtils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @Title
 * @Author dragon
 * @Description
 * @Date 2023/11/28 15:24
 **/
public class MysqlStockReader extends Reader {

    private static Logger log = LoggerFactory.getLogger(MysqlStockReader.class);

    private static final String READER_SQL = "SELECT * FROM %s ;";

    private static final Integer LIMIT = 1000;

    private static final Integer TABLE_JOB_LIMIT = 10;

    private String dbName;

    private List<String> tables;

    private DataSource source;

    private LocalTransport transport;

    private Boolean force = Boolean.FALSE;
    /**
     * 执行任务
     */
    ExecutorService jobExecutor;

    List<Future<Boolean>> jobList;


    public MysqlStockReader(DataSource source, String dbName, LocalTransport transport) {
        this.dbName = dbName;
        this.source = source;
        this.transport = transport;

    }

    public Boolean getForce() {
        return force;
    }

    public void setForce(Boolean force) {
        this.force = force;
    }

    public void init() {
        try {
            this.tables = SchemaManager.getDbTalesInfo(source.getConnection());
            jobList = new ArrayList<>(tables.size());
            this.jobExecutor = Executors.newFixedThreadPool(
                    tables.size() > TABLE_JOB_LIMIT ? TABLE_JOB_LIMIT : tables.size(),
                    new ThreadFactoryBuilder()
                            .setNameFormat(this.dbName + "-Executors-%d")
                            .setDaemon(true)
                            .build()
            );
        } catch (SQLException e) {
            throw new InvalidTableException(DBErrorCode.DB_READ_FAIL, "前期初始化失败" + dbName, e);
        }
    }


    @Override
    public void startReader() {
        try {

            init();
            for (String table : tables) {

                Future<Boolean> future = null;
                try {
                    future = jobExecutor.submit(new MysqlReaderJob(table, source.getConnection()));
                    jobList.add(future);
                } catch (SQLException e) {
                    if (Objects.nonNull(future)) {
                        future.cancel(true);
                    }
                    throw new InvalidTableException(TableErrorCode.TABLE_FIELD_EMPTY, String.format("数据库同步 表 %s", table), e);
                }
            }
            for (Future future : jobList) {
                future.get();
            }
            log.info("发送结束");
            transport.push(new EndRecord());
        } catch (Exception e) {
            if (Objects.nonNull(jobList) && !jobList.isEmpty()) {
                for (Future future : jobList) {
                    if (future.isDone()) {
                        continue;
                    }
                    future.cancel(true);
                }
            }
            throw new InvalidTableException(DBErrorCode.DB_READ_FAIL, String.format("数据库同步失败 %s", dbName), e);
        }
    }


    class MysqlReaderJob extends Reader.Job {
        /**
         * 处理表效果
         */
        private String table;
        /**
         * 连接
         */
        private Connection conn;

        public MysqlReaderJob(String table, Connection conn) {
            this.table = table;
            this.conn = conn;
        }

        @Override
        protected void preCheck() {
            try {
                DatabaseMetaData metaData = conn.getMetaData();
                ResultSet resultSet = metaData.getPrimaryKeys(null, null, table);
                String primaryKey = "";
                while (resultSet.next()) {
                    primaryKey = resultSet.getString("COLUMN_NAME");
                }
                if (StringUtils.isBlank(primaryKey)) {
                    throw InvalidTableException.asException(TableErrorCode.TABLE_NOT_PRI, null);
                }
            } catch (SQLException e) {
                throw DragonTException.asException(TableErrorCode.TABLE_NOT_PRI, "主键校验失败", e);
            }
        }

        @Override
        protected void preHandle() {
            try {
                Statement stat = this.conn.createStatement();
                ResultSet rs = DBUtils.query(stat, "show create table " + table);
                StructRecord structRecord = new StructRecord();
                while (rs.next()) {
                    String createTable = rs.getString("CREATE TABLE");
                    structRecord.addColumn(new StructColum(createTable));
                    structRecord.setForce(force);
                    structRecord.setTbName(table);
                    //传输通道
                    transport.push(structRecord);
                }
            } catch (SQLException e) {
                throw DragonTException.asException(TableErrorCode.TABLE_FIELD_EMPTY, "读取表结构出现错误", e);
            }
        }

        @Override
        protected void afterHandle() {
            DoneRecord doneRecord = new DoneRecord();
            doneRecord.setTbName(table);
            transport.push(doneRecord);
            log.info("db {} table {} stock send end", dbName, table);
        }

        @Override
        protected void read() {
            readTableWithConnect();
        }

        private void readTableWithConnect() {
            try {

                String sql = String.format(READER_SQL, table);
                ResultSet rs = DBUtils.query(conn, sql, LIMIT, Constant.SOCKET_TIMEOUT_INSECOND);
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                while (rs.next()) {
                    Record record = transportOneRecord(rs, metaData, columnCount);
                    record.setTbName(table);
                    transport.push(record);
                }
            } catch (SQLException e) {
                throw new InvalidTableException(TableErrorCode.TABLE_FIELD_EMPTY, String.format("数据库同步 表 %s", table), e);
            }
        }

        private Record transportOneRecord(ResultSet rs, ResultSetMetaData metaData, int columnCount) {
            Record record = new StockRecord();
            try {
                for (int i = 1; i <= columnCount; i++) {
                    Column column = null;
                    switch (metaData.getColumnType(i)) {
                        case Types.CHAR:
                        case Types.NCHAR:
                        case Types.VARCHAR:
                        case Types.LONGVARCHAR:
                        case Types.NVARCHAR:
                        case Types.LONGNVARCHAR:
                            column = new StringColumn(rs.getString(i));
                            break;
                        case Types.CLOB:
                        case Types.NCLOB:
                            column = new StringColumn(rs.getString(i));
                            break;
                        case Types.SMALLINT:
                        case Types.TINYINT:
                        case Types.INTEGER:
                        case Types.BIGINT:
                            column = new LongColumn(rs.getString(i));
                            break;
                        case Types.NUMERIC:
                        case Types.DECIMAL:
                            column = new DoubleColumn(rs.getString(i));
                            break;
                        case Types.FLOAT:
                        case Types.REAL:
                        case Types.DOUBLE:
                            column = new DoubleColumn(rs.getString(i));
                            break;
                        case Types.TIME:
                            column = new DateColumn(rs.getTime(i));
                            break;
                        // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
                        case Types.DATE:
                            if (metaData.getColumnTypeName(i).equalsIgnoreCase("year")) {
                                column = new LongColumn(rs.getInt(i));
                            } else {
                                column = new DateColumn(rs.getDate(i));
                            }
                            break;
                        case Types.TIMESTAMP:
                            column = new DateColumn(rs.getTimestamp(i));
                            break;
                        case Types.BINARY:
                        case Types.VARBINARY:
                        case Types.BLOB:
                        case Types.LONGVARBINARY:
                            column = new BytesColumn(rs.getBytes(i));
                            break;
                        // warn: bit(1) -> Types.BIT 可使用BoolColumn
                        // warn: bit(>1) -> Types.VARBINARY 可使用BytesColumn
                        case Types.BOOLEAN:
                        case Types.BIT:
                            column = new BoolColumn(rs.getBoolean(i));
                            break;

                        case Types.NULL:
                            String stringData = null;
                            if (rs.getObject(i) != null) {
                                stringData = rs.getObject(i).toString();
                            }
                            column = new StringColumn(stringData);
                            break;
                        default:
                            throw DragonTException.asException(CommonErrorCode.UNSUPPORTED_TYPE,
                                    String.format(
                                            "您的配置文件中的列配置信息有误.  字段名:[%s], 字段名称:[%s], 字段Java类型:[%s]. ",
                                            metaData.getColumnName(i),
                                            metaData.getColumnType(i),
                                            metaData.getColumnClassName(i)));
                    }
                    record.addColumn(column);
                }
            } catch (Exception e) {
                if (e instanceof DragonTException) {
                    throw (DragonTException) e;
                }
                throw DragonTException.asException(CommonErrorCode.UNKNOWN_ERROR, e);
            }
            return record;
        }
    }


}
