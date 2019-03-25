package ch.epfl.dias.store.PAX;

import java.util.ArrayList;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class DBPAXpage {
	private ArrayList<DBColumn> columns;
	private DataType[] types;

	public DBPAXpage(Object[][] fields, DataType[] types) {
		this.columns = new ArrayList<DBColumn>();
		this.types = types;

		for (int j = 0; j < fields[0].length; j++) {
			ArrayList<Object> values = new ArrayList<Object>();
			for (int i = 0; i < fields.length; i++) {
				values.add(fields[i][j]);
			}
			DBColumn column = new DBColumn(values.toArray(), types[j]);
			columns.add(column);
		}
	}

	public DBTuple getTuple(int rowNumber) {
		Object[] fields = new Object[columns.size()];
		for (int i = 0; i < columns.size(); i++) {
			switch (this.types[i]) {
			case INT:
				fields[i] = columns.get(i).getAsInteger()[rowNumber];
				break;
			case DOUBLE:
				fields[i] = columns.get(i).getAsDouble()[rowNumber];
				break;
			case BOOLEAN:
				fields[i] = columns.get(i).getAsBoolean()[rowNumber];
				break;
			case STRING:
				fields[i] = columns.get(i).getAsString()[rowNumber];
				break;
			default:
				// ????????????????
				fields[i] = columns.get(i).getAsString()[rowNumber];
				break;
			}

		}

		return new DBTuple(fields, types);
	}
}
