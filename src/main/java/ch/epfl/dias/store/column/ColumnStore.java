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
  private int columnSize = 0;
	private List<DBColumn> columns;

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
		List<List<Object>> temp = new ArrayList<ArrayList<Object>>();

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
			columns.add(new DBColumn(temp.get(i), this.schema[i]));
		}
    columnSize = columns[0].getSize();
		if (reader != null) {
			reader.close();
		}
	}

	@Override
	public DBColumn[] getColumns(int[] columnsToGet) {
		DBColumn[] res = new DBColumn[columnsToGet.size];
    for (int i = 0; i < columnsToGet.size; i++) {
      res[i] = columns.get(columnsToGet[i]);
    }
		return res;
	}

  public int getColumnSize() {
    return columnSize;
  }

  public DBColumn[] getColumns() {
    return columns;
  }

  public DBColumn[] getColumnsVector(int fromIndex, int toIndex) {
    DBColumn[] res = new DBColumn[columns.size];
    for (int i = 0; i < columnsToGet.size; i++) {
      res[i] = columns.get(i).subList(fromIndex, toIndex);
    }
    return res;
  }
}
