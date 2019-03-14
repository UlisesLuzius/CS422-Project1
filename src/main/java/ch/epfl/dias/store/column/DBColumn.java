package ch.epfl.dias.store.column;

//import java.util.ArrayList;
import java.util.Arrays;

//import ch.epfl.dias.store.DataType;

public class DBColumn {

	private Object[] values;
	//private DataType type;
	
	public DBColumn(Object[] values/**, DataType type**/) {
		this.values = values;
		//this.type = type;
	}
	
	/**public DataType getType() {
		return this.type;
	}**/
	
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
}
