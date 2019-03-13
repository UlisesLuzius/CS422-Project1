package ch.epfl.dias.store.row;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

public class RowStore extends Store {

	private DataType[] schema;
	private String filename;
	private String delimiter;

	private ArrayList<DBTuple> tuples;

	public RowStore(DataType[] schema, String filename, String delimiter) {
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
		this.tuples = new ArrayList<DBTuple>();
	}

	@Override
	public void load() throws IOException {
		File file = new File("input/" + this.filename);
		BufferedReader reader = null;

		try {
			reader = new BufferedReader(new FileReader(file));
			String text;
			while ((text = reader.readLine()) != null) {
				String[] fields = text.split(delimiter);
				Object[] convertedFields = new Object[schema.length];
				for (int i = 0; i < schema.length; i++) {
					switch (schema[i]) {
					case INT:
						convertedFields[i] = Integer.parseInt(fields[i]);
					case DOUBLE:
						convertedFields[i] = Double.parseDouble(fields[i]);
					case BOOLEAN:
						convertedFields[i] = Boolean.parseBoolean(fields[i]);
					case STRING:
						convertedFields[i] = fields[i];
					default:
						// ????????????????
						convertedFields[i] = fields[i];
					}
				}
				DBTuple tuple = new DBTuple(convertedFields, this.schema);
				this.tuples.add(tuple);
			}
			this.tuples.add(new DBTuple());
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (reader != null) {
					reader.close();
				}
			} catch (IOException e) {
			}
		}

	}

	@Override
	public DBTuple getRow(int rownumber) {
		if (rownumber < 0 || rownumber >= tuples.size()) {
			return null;
		}
		
		return tuples.get(rownumber);
	}
}
