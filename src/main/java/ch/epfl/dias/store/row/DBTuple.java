package ch.epfl.dias.store.row;

import ch.epfl.dias.store.DataType;

public class DBTuple {
	private Object[] fields;
	private DataType[] types;
	public boolean eof;

	public DBTuple(Object[] fields, DataType[] types) {
		this.fields = fields;
		this.types = types;
		this.eof = false;
	}

	public DBTuple() {
		this.eof = true;
	}
	
	public DataType[] getTypes() {
		return this.types;
	}
	
	public Object[] getFields() {
		return this.fields;
	}
	
	public Integer getFieldAsInt(int fieldNo) {
		return (Integer) fields[fieldNo];
	}

	public Double getFieldAsDouble(int fieldNo) {
		return (Double) fields[fieldNo];
	}

	public Boolean getFieldAsBoolean(int fieldNo) {
		return (Boolean) fields[fieldNo];
	}

	public String getFieldAsString(int fieldNo) {
		return (String) fields[fieldNo];
	}
	
	public Object getFieldAsObject(int fieldNo) {
		return fields[fieldNo];
	}
}
