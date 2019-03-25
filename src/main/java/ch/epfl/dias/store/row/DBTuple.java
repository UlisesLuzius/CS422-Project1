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
	
	public DBTuple(DBTuple left, DBTuple right, int rightFieldNo) {
		Object[] leftFields = left.getFields();
		Object[] rightFields = right.getFields();
		int leftLength = leftFields.length;
		int rightLength = rightFields.length;

		DataType[] leftTypes = left.getTypes();
		DataType[] rightTypes = right.getTypes();

		int finalLength = leftLength + rightLength - 1;
		Object[] finalFields = new Object[finalLength];
		DataType[] finalDataTypes = new DataType[finalLength];


		for (int i = 0; i < leftLength; i++) {
			finalFields[i] = leftFields[i];
			finalDataTypes[i] = leftTypes[i];
		}

		for (int i = 0; i < rightFieldNo; i++) {
			finalFields[leftLength + i] = rightFields[i];
			finalDataTypes[leftLength + i] = rightTypes[i];
		}
		for (int i = rightFieldNo + 1; i < rightLength; i++) {
			finalFields[leftLength + i - 1] = rightFields[i];
			finalDataTypes[leftLength + i - 1] = rightTypes[i];
		}
		
		this.fields = finalFields;
		this.types = finalDataTypes;
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
