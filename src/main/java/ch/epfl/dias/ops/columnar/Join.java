package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Join implements ColumnarOperator {

	private ColumnarOperator leftChild;
	private ColumnarOperator rightChild;
	private int leftFieldNo;
	private int rightFieldNo;

	public Join(ColumnarOperator leftChild, ColumnarOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
	}

	public DBColumn[] execute() {
		DBColumn[] left = leftChild.execute();
		DBColumn[] right = rightChild.execute();

		int leftLength = left[0].size();

		HashMap<Integer, ArrayList<Integer>> rightMap = this.buildHashTable(right);
		HashMap<Integer, ArrayList<Integer>> leftMap = this.buildHashTable(left);

		DBColumn[] result = this.initialize(left, right);

		for (int i = 0; i < leftLength; i++) {
			int value = (int) left[leftFieldNo].get(i);
			ArrayList<Integer> rightIndices = rightMap.get(value);
			// filling all the new tuples where left index is i
			for (int j = 0; j < rightIndices.size(); j++) {
				// filling columns 0 to left.length
				for (int k = 0; k < left.length; k++) {
					result[k].add(left[k].get(i));
				}
				// filling columns left.length to left.length + rightFieldNo
				for (int k = 0; k < rightFieldNo; k++) {
					result[k + left.length].add(right[k].get(rightIndices.get(j)));
				}
				// filling columns left.length + rightFieldNo to end
				for (int k = rightFieldNo + 1; k < right.length; k++) {
					result[k + left.length - 1].add(right[k].get(rightIndices.get(j)));
				}

			}
		}

		return result;
	}

	private HashMap<Integer, ArrayList<Integer>> buildHashTable(DBColumn[] rightColumns) {
		HashMap<Integer, ArrayList<Integer>> hashMap = new HashMap<Integer, ArrayList<Integer>>();
		DBColumn right = rightColumns[this.rightFieldNo];

		Integer[] rightValues = right.getAsInteger();

		for (int i = 0; i < rightValues.length; i++) {
			int currentValue = rightValues[i];
			ArrayList<Integer> indices = hashMap.getOrDefault(currentValue, new ArrayList<Integer>());
			indices.add(i);
			hashMap.put(currentValue, indices);
		}
		return hashMap;
	}

	private DBColumn[] initialize(DBColumn[] left, DBColumn[] right) {
		List<DBColumn> columns = new ArrayList<DBColumn>();

		for (int i = 0; i < left.length; i++) {
			columns.add(new DBColumn(left[i].getType()));
		}
		for (int i = 0; i < rightFieldNo; i++) {
			columns.add(new DBColumn(right[i].getType()));
		}
		for (int i = rightFieldNo + 1; i < right.length; i++) {
			columns.add(new DBColumn(right[i].getType()));
		}
		return columns.toArray(new DBColumn[] {});
	}
}
