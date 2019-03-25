package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class Join implements VectorOperator {

	private VectorOperator leftChild;
	private VectorOperator rightChild;
	private int leftFieldNo;
	private int rightFieldNo;
	private int leftIdx;
	private int rightIdx;
	DBColumn[] leftColumns;
	DBColumn[] rightColumns;

	public Join(VectorOperator leftChild, VectorOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
	}

	@Override
	public void open() {
		leftChild.open();
		rightChild.open();
		leftIdx = 0;
		rightIdx = 0;
	}

	@Override
	public DBColumn[] next() {
		if (leftIdx == 0) {
			leftColumns = leftChild.next();
			if (leftColumns == null) {
				return null;
			}
		}
		if (rightIdx == 0) {
			rightColumns = rightChild.next();
			if (rightColumns == null) {
				return null;
			}
		}
		DBColumn[] res = new DBColumn[leftColumns.length + rightColumns.length];
		for (int i = 0; i < leftColumns.length + rightColumns.length - 1; i++) {
			if (i < leftColumns.length) {
				res[i] = new DBColumn(leftColumns[i].getType());
			} else if (i < leftColumns.length + rightFieldNo) {
				res[i] = new DBColumn(rightColumns[i - leftColumns.length].getType());
			} else {
				res[i] = new DBColumn(rightColumns[i - leftColumns.length + 1].getType());
			}
		}
		int vectorsize = Math.max(leftColumns[leftFieldNo].size(), rightColumns[rightFieldNo].size());
		int matchs = 0;
		while (leftColumns != null && matchs < vectorsize) {
			HashMap<Integer, ArrayList<Integer>> hashTable = buildHashTable(leftColumns[leftFieldNo]);

			while (rightIdx < rightColumns[rightFieldNo].size() && matchs < vectorsize) {
				ArrayList<Integer> indices = hashTable.getOrDefault(rightColumns[rightFieldNo].get(rightIdx), null);
				if (indices != null) {
					for (int j = 0; j < indices.size(); j++) {
						for (int i = 0; i < leftColumns.length; i++) {
							res[i].add(leftColumns[i].get(indices.get(j)));
						}
						for (int i = leftColumns.length; i < leftColumns.length + rightFieldNo; i++) {
							res[i].add(rightColumns[i - leftColumns.length].get(rightIdx));
						}
						for (int i = leftColumns.length + rightFieldNo; i < leftColumns.length + rightColumns.length
								- 1; i++) {
							res[i].add(rightColumns[i - leftColumns.length + 1].get(rightIdx));
						}
					}
				}
				rightColumns = rightChild.next();
				rightIdx++;
			}

			if (matchs < vectorsize) {
				leftColumns = leftChild.next();
				leftIdx++;
				rightChild.close();
				rightChild.open();
				rightIdx = 0;
			}
		}
		return res;
	}

	@Override
	public void close() {
		leftChild.close();
		rightChild.close();
		leftIdx = 0;
		rightIdx = 0;
	}

	private HashMap<Integer, ArrayList<Integer>> buildHashTable(DBColumn column) {
		HashMap<Integer, ArrayList<Integer>> hashMap = new HashMap<Integer, ArrayList<Integer>>();

		Integer[] rightValues = column.getAsInteger();

		for (int i = 0; i < rightValues.length; i++) {
			int currentValue = rightValues[i];
			ArrayList<Integer> indices = hashMap.getOrDefault(currentValue, new ArrayList<Integer>());
			indices.add(i);
			hashMap.put(currentValue, indices);
		}
		return hashMap;
	}
}
