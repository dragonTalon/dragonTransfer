package com.dragon.transfer.common.element;

import com.alibaba.fastjson2.JSON;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

/**
 * Created by jingxing on 14-8-24.
 * <p/>
 */
public abstract class Column {

	private Type type;

	private Object rawData;

	private int byteSize;

	private int columnSqlType;

	public Column(final Object object, final Type type, int byteSize) {
		this.rawData = object;
		this.type = type;
		this.byteSize = byteSize;
	}

	public int getColumnSqlType() {
		return columnSqlType;
	}

	public void setColumnSqlType(int columnSqlType) {
		this.columnSqlType = columnSqlType;
	}

	public Object getRawData() {
		return this.rawData;
	}

	public Type getType() {
		return this.type;
	}

	public int getByteSize() {
		return this.byteSize;
	}

	protected void setType(Type type) {
		this.type = type;
	}

	protected void setRawData(Object rawData) {
		this.rawData = rawData;
	}

	protected void setByteSize(int byteSize) {
		this.byteSize = byteSize;
	}

	public abstract Long asLong();

	public abstract Double asDouble();

	public abstract String asString();

	public abstract Date asDate();
	
	public abstract Date asDate(String dateFormat);

	public abstract byte[] asBytes();

	public abstract Boolean asBoolean();

	public abstract BigDecimal asBigDecimal();

	public abstract BigInteger asBigInteger();

	@Override
	public String toString() {
		return JSON.toJSONString(this);
	}

	public enum Type {
		/**
		 *结构化
		 */
		STRUCT,
		BAD,
		NULL,
		INT,
		LONG,
		DOUBLE,
		STRING,
		BOOL,
		DATE,
		BYTES,

	}
}
