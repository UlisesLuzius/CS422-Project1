package ch.epfl.dias.store.column;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

public class ColumnStore extends Store {
	private DataType[] schema;
	private String filename;
	private String delimiter;
	private ArrayList<DBColumn> columns;
	// TODO: Add required structures

	public ColumnStore(DataType[] schema, String filename, String delimiter) {
		this(schema, filename, delimiter, false);
	}

	public ColumnStore(DataType[] schema, String filename, String delimiter, boolean lateMaterialization) {
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
		this.columns = new ArrayList<DBColumn>();
	}

	@Override
	public void load() throws IOException {
		File file = new File("input/" + this.filename);
		BufferedReader reader = null;

		reader = new BufferedReader(new FileReader(file));
		String text;
		ArrayList<ArrayList<Object>> temp = new ArrayList<ArrayList<Object>>();

		while ((text = reader.readLine()) != null) {
			String[] fields = text.split(delimiter);
			for (int i = 0; i < this.schema.length; i++) {
				switch (this.schema[i]) {
				case INT:
					temp.get(i).add(Integer.parseInt(fields[i]));
					break;
				case DOUBLE:
					temp.get(i).add(Double.parseDouble(fields[i]));
					break;
				case BOOLEAN:
					temp.get(i).add(Boolean.parseBoolean(fields[i]));
					break;
				case STRING:
					temp.get(i).add(fields[i]);
					break;
				default:
					// ????????????????
					temp.get(i).add(fields[i]);
					break;
				}
			}
		}
		for (int i = 0; i < this.schema.length; i++) {
			columns.add(new DBColumn(temp.get(i).toArray(), this.schema[i]));
		}
		if (reader != null) {
			reader.close();
		}
	}

	@Override
	public DBColumn[] getColumns(int[] columnsToGet) {
		int length = columnsToGet.length;
		DBColumn[] result;
		if (length == 0) {
			result = new DBColumn[columns.size()];
			for (int i = 0; i < this.columns.size(); i++) {
				result[i] = columns.get(i);
			}
		} else {
			result = new DBColumn[columnsToGet.length];
			for (int i = 0; i < columnsToGet.length; i++) {
				result[i] = columns.get(columnsToGet[i]);
			}
		}
		return result;
	}
}
