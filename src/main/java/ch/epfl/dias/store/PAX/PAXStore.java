package ch.epfl.dias.store.PAX;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class PAXStore extends Store {

	// TODO: Add required structures
	DataType[] schema;
	String filename;
	String delimiter;
	int tuplesPerPage;
	ArrayList<DBPAXpage> pages;

	public PAXStore(DataType[] schema, String filename, String delimiter, int tuplesPerPage) {
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
		this.tuplesPerPage = tuplesPerPage;
		this.pages = new ArrayList<DBPAXpage>();
	}

	@Override
	public void load() throws IOException {
		File file = new File("input/" + this.filename);
		BufferedReader reader = null;

		reader = new BufferedReader(new FileReader(file));
		String text;

		ArrayList<Object[]> rows = new ArrayList<Object[]>();
		while ((text = reader.readLine()) != null) {
			String[] fields = text.split(delimiter);
			rows.add(fields);
		}

		int numPages = (int) Math.ceil((double) rows.size() / (double) tuplesPerPage);

		Object[][] temp = (Object[][]) rows.toArray();
		for (int i = 0; i < numPages; i++) {
			Object[][] pageArray = Arrays.copyOfRange(temp, i * tuplesPerPage, (i + 1) * tuplesPerPage);
			pages.add(new DBPAXpage(pageArray, this.schema));
		}

		if (reader != null) {
			reader.close();
		}
	}

	@Override
	public DBTuple getRow(int rownumber) {
		int pageNum = (int) Math.floor((double) rownumber / (double) tuplesPerPage);
		int indexInPage = rownumber - pageNum * tuplesPerPage;
		return pages.get(pageNum).getTuple(indexInPage);
	}
}
