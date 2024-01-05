package com.dragon.transfer.core;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.dragon.transfer.common.enums.DBType;
import com.dragon.transfer.core.handle.recevie.mysql.MysqlStockReader;
import com.dragon.transfer.core.handle.sink.mysql.MysqlStockWriter;
import com.dragon.transfer.core.handle.transport.LocalTransport;
import com.dragon.transfer.core.utils.BinLogSchema;
import com.dragon.transfer.core.utils.DBUtils;

import java.sql.SQLException;

/**
 * @Title
 * @Author dragon
 * @Description
 * @Date 2023/11/28 09:58
 **/
public class DemoTest {
    private static DruidDataSource source;

    private static DruidDataSource target;

    private static final String dbName = "";

    public static void main(String[] args) throws SQLException, InterruptedException {
        initSource();

        DruidPooledConnection conn = source.getConnection();

        BinLogSchema binlogPos = DBUtils.getBinlogPos(conn);
        LocalTransport transport = new LocalTransport(dbName);
        MysqlStockReader reader = new MysqlStockReader(source, dbName, transport);
        reader.setForce(true);
        new Thread(() -> {
            reader.startReader();
        }).start();

        initTarget();
        //全量同步
        MysqlStockWriter mysqlStockWriter = new MysqlStockWriter(target, transport, dbName);
        mysqlStockWriter.startWriter();

    }

    private static void initSource() {
        source = new DruidDataSource();
        source.setDriverClassName(DBType.MYSQL.getDriverClass());
        /**设置数据库连接地址**/
        source.setUrl("");
        /**设置数据库连接用户名**/
        source.setUsername("");
        /**设置数据库连接密码**/
        source.setPassword("");

        /**初始化时创建的连接数,应在minPoolSize与maxPoolSize之间取值.默认为3**/
        source.setInitialSize(3);
        source.setMinIdle(1);
        /**连接池中保留的最大连接数据.默认为15**/
        source.setMaxActive(200);
        //配置间隔多久才进行一次检测，检测需要关闭的空闲连接，单位是毫秒
        source.setTimeBetweenEvictionRunsMillis(10000);
        //防止过期
        source.setValidationQuery("SELECT 1 FROM DUAL");
        //连接泄漏监测
        source.setRemoveAbandoned(true);
        source.setRemoveAbandonedTimeout(30);
        //配置获取连接等待超时的时间
        source.setMaxWait(10000);
        source.setTestWhileIdle(true);
        source.setTestOnBorrow(true);
        source.setPoolPreparedStatements(false);
        source.setKeepAlive(true);
    }

    private static void initTarget() {
        target = new DruidDataSource();
        target.setDriverClassName(DBType.MYSQL.getDriverClass());
        /**设置数据库连接地址**/
        target.setUrl("");
        /**设置数据库连接用户名**/
        target.setUsername("");
        /**设置数据库连接密码**/
        target.setPassword("");

        /**初始化时创建的连接数,应在minPoolSize与maxPoolSize之间取值.默认为3**/
        target.setInitialSize(3);
        target.setMinIdle(1);
        /**连接池中保留的最大连接数据.默认为15**/
        target.setMaxActive(200);
        //配置间隔多久才进行一次检测，检测需要关闭的空闲连接，单位是毫秒
        target.setTimeBetweenEvictionRunsMillis(10000);
        //防止过期
        target.setValidationQuery("SELECT 1 FROM DUAL");
        //连接泄漏监测
        target.setRemoveAbandoned(true);
        target.setRemoveAbandonedTimeout(30);
        //配置获取连接等待超时的时间
        target.setMaxWait(10000);
        target.setTestWhileIdle(true);
        target.setTestOnBorrow(true);
        target.setPoolPreparedStatements(false);
        target.setKeepAlive(true);
    }
}
