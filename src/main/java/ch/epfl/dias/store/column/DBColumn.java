package ch.epfl.dias.store.column;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

import ch.epfl.dias.store.DataType;

public class DBColumn {

	private List<Object> values;
	private DataType type;
	private int size;

	public DBColumn(Object[] values, DataType type) {
		this.values = Arrays.asList(values);
		this.type = type;
		this.size = values.length;
	}

	public DBColumn(DataType type) {
		this.values = new ArrayList<Object>();
		this.type = type;
		this.size = 0;
	}

	public DBColumn() {
  }

  public void setType(DataType type) {
    this.type = type;
  }

	public void add(Object value) {
		this.values.add(value);
		this.size++;
	}

	public DataType getType() {
		return type;
	}

	public int size() {
		return size;
	}

	public Object get(int i) {
		return values.get(i);
	}

	public Object[] getAsObject() {
		return values.toArray();
	}

	public Integer[] getAsInteger() {
		return Arrays.asList(values).toArray(new Integer[0]);
	}

	public Double[] getAsDouble() {
		return Arrays.asList(values).toArray(new Double[0]);
	}

	public Boolean[] getAsBoolean() {
		return Arrays.asList(values).toArray(new Boolean[0]);
	}

	public String[] getAsString() {
		return Arrays.asList(values).toArray(new String[0]);
	}

}
