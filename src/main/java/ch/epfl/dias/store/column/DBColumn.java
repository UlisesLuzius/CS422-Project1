package ch.epfl.dias.store.column;

import java.util.ArrayList;
import java.util.Arrays;

import ch.epfl.dias.store.DataType;

public class DBColumn {

	private ArrayList<Object> values;
	private DataType type;
	private int size;

	public DBColumn(Object[] values, DataType type) {
		this.values = new ArrayList<Object>(Arrays.asList(values));
		this.type = type;
		this.size = values.length;
	}

	public DBColumn(DataType type) {
		this.values = new ArrayList<Object>();
		this.type = type;
		this.size = 0;
	}

	public void add(Object value) {
		this.values.add(value);
		this.size++;
	}

	public DataType getType() {
		return this.type;
	}

	public int size() {
		return this.size;
	}

	public Integer[] getAsInteger() {
		return Arrays.asList(this.values).toArray(new Integer[0]);
	}

	public Double[] getAsDouble() {
		return Arrays.asList(this.values).toArray(new Double[0]);
	}

	public Boolean[] getAsBoolean() {
		return Arrays.asList(this.values).toArray(new Boolean[0]);
	}

	public String[] getAsString() {
		return Arrays.asList(this.values).toArray(new String[0]);
	}

	public Object[] getAsObject() {
		return Arrays.asList(this.values).toArray(new Object[0]);
	}
}
