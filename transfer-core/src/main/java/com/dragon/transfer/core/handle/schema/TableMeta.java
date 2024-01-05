package com.dragon.transfer.core.handle.schema;

import lombok.Data;

import java.util.List;

/**
 * @Title
 * @Author dragon
 * @Description
 * @Date 2023/11/28 10:24
 **/
@Data
public class TableMeta {
    private String tbName;
    private List<FieldSchema> cols;

}
