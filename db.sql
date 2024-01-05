#
同步任务
create table replicate_job
(
    id             bigint(20) primary key auto_increment,
    task_name      varchar(128)                       not null comment 'task name',
    source_db_id   bigint null comment '同步源数据库id',
    sink_db_id     bigint null comment '消费者数据库ID',
    table_excludes longtext null comment '不包括(指定排除填写db_name.* ; 不做排除 填写空 "" ,excludes 规则优先级高于 includes)',
    table_includes longtext null comment '包括(全部填 *，指定某个数据库所有表填写 db_name.*)',
    log_file_name  varchar(255) null comment '读取的日志文件名',
    start_position bigint null comment '读取日志的开始位置',
    create_time    datetime default CURRENT_TIMESTAMP null comment '创建时间',
    modify_time    datetime default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP comment '修改时间'
);

#
实际执行任务
create table task
(
    id          bigint(20) primary key auto_increment,
    job_id bigint(20) not null comment 'replicate task id',
    type        varchar(32)                        not null comment 'executor type',
    status      varchar(32)                        not null comment 'executor status',
    next_task_id bigint(20) default 0 comment 'next task  id',
    create_time datetime default CURRENT_TIMESTAMP null comment '创建时间',
    modify_time datetime default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP comment '修改时间'
);

#
数据库信息
create table database_info
(
    id                bigint auto_increment primary key,
    db_name           varchar(50) null comment '数据库名称',
    host              varchar(200)                       not null comment '数据库主机ip',
    port              int(20) null comment '数据库连接端口',
    user_name         varchar(50) null comment '数据库连接用户名',
    password          varchar(50) null comment '数据库密码',
    db_type           varchar(30) null comment '数据库类型',
    driver_class_name varchar(100) null comment '驱动类名',
    region            varchar(100) null comment '区域标志',
    idc               varchar(20) null,
    conn_url          varchar(200) null comment '特殊情况，直接使用url拼接链接数据库',
    create_time       datetime default CURRENT_TIMESTAMP null comment '创建时间',
    modify_time       datetime default CURRENT_TIMESTAMP not null on update CURRENT_TIMESTAMP comment '修改时间'
) comment '数据库信息表';

