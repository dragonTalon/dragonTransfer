package com.dragon.transfer.core.handle.record;

import com.dragon.transfer.common.element.Column;
import com.dragon.transfer.common.element.Record;
import com.dragon.transfer.common.enums.RecordType;

import java.util.Map;
import java.util.Objects;

/**
 * @Title
 * @Author dragon
 * @Description
 * @Date 2023/11/29 16:34
 **/
public class StructRecord implements Record {
    private String tbName;

    private Column column;

    private Boolean force = Boolean.FALSE;


    public Boolean getForce() {
        return force;
    }

    public void setForce(Boolean force) {
        this.force = force;
    }

    @Override
    public void addColumn(Column column) {
        this.column = column;
    }

    @Override
    public void setColumn(int i, Column column) {
        return;
    }

    @Override
    public Column getColumn(int i) {
        return column;
    }

    @Override
    public String getTbName() {
        return this.tbName;
    }

    @Override
    public void setTbName(String tbName) {
        this.tbName = tbName;
    }

    @Override
    public int getColumnNumber() {
        return Objects.nonNull(column) ? 1 : 0;
    }

    @Override
    public RecordType getType() {
        return RecordType.STRUCT;
    }


}
