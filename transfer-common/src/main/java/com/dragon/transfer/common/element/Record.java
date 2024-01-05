package com.dragon.transfer.common.element;

import com.dragon.transfer.common.enums.RecordType;

import java.util.Map;

/**
 * Created by jingxing on 14-8-24.
 */

public interface Record {

     void addColumn(Column column);

     void setColumn(int i, final Column column);

     Column getColumn(int i);

     String getTbName();

     void setTbName(String tbName);

     String toString();

     int getColumnNumber();

    RecordType getType();

}
