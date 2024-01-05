package com.dragon.transfer.core.handle.record;

import com.alibaba.fastjson2.JSON;
import com.dragon.transfer.common.element.Column;
import com.dragon.transfer.common.element.Record;
import com.dragon.transfer.common.enums.RecordType;
import com.dragon.transfer.common.exception.DragonTException;
import com.dragon.transfer.common.exception.code.CommonErrorCode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Title:全量记录
 * @Author dragon
 * @Description
 * @Date 2023/11/29 16:17
 **/
public class StockRecord implements Record {

    private static final int RECORD_AVERGAE_COLUMN_NUMBER = 16;

    private List<Column> columns;

    private String tbName;

    public StockRecord() {
        this.columns = new ArrayList<Column>(RECORD_AVERGAE_COLUMN_NUMBER);
    }

    @Override
    public String getTbName() {
        return tbName;
    }

    @Override
    public void setTbName(String tbName) {
        this.tbName = tbName;
    }

    @Override
    public void addColumn(Column column) {
        columns.add(column);
    }

    @Override
    public void setColumn(int i, Column column) {
        if (i < 0) {
            throw DragonTException.asException(CommonErrorCode.ARGUMENT_ERROR,
                    "不能给index小于0的column设置值");
        }

        if (i >= columns.size()) {
            expandCapacity(i + 1);
        }
        this.columns.set(i, column);
    }

    @Override
    public Column getColumn(int i) {
        if (i < 0 || i >= columns.size()) {
            return null;
        }
        return columns.get(i);
    }

    @Override
    public int getColumnNumber() {
        return this.columns.size();
    }

    @Override
    public RecordType getType() {
        return RecordType.ONLY_INSERT;
    }

    @Override
    public String toString() {
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("size", this.getColumnNumber());
        json.put("data", this.columns);
        return JSON.toJSONString(json);
    }

    private void expandCapacity(int totalSize) {
        if (totalSize <= 0) {
            return;
        }

        int needToExpand = totalSize - columns.size();
        while (needToExpand-- > 0) {
            this.columns.add(null);
        }
    }
}
