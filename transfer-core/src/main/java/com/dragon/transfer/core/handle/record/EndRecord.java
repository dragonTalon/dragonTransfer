package com.dragon.transfer.core.handle.record;

import com.dragon.transfer.common.element.Column;
import com.dragon.transfer.common.element.Record;
import com.dragon.transfer.common.enums.RecordType;

/**
 * @Title
 * @Author dragon
 * @Description
 * @Date 2023/11/30 16:56
 **/
public class EndRecord implements Record {
    @Override
    public void addColumn(Column column) {

    }

    @Override
    public void setColumn(int i, Column column) {

    }

    @Override
    public Column getColumn(int i) {
        return null;
    }

    @Override
    public String getTbName() {
        return null;
    }

    @Override
    public void setTbName(String tbName) {

    }

    @Override
    public int getColumnNumber() {
        return 0;
    }

    @Override
    public RecordType getType() {
        return RecordType.END;
    }
}
