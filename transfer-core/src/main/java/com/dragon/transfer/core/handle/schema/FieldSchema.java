package com.dragon.transfer.core.handle.schema;

import lombok.Data;

/**
 * @Title
 * @Author dragon
 * @Description
 * @Date 2023/11/28 10:26
 **/
@Data
public class FieldSchema {
    /**
     * 列名
     */
    private String name;

    /**
     * 列类型，如：string, bigint, boolean, datetime等等
     */
    private String javaType;

    private String type;
    /**
     * 主键
     */
    private Boolean pri = Boolean.FALSE;
}
