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
		int vectorsize = Math.max(leftColumns[leftFieldNo].size(), rightColumns[rightFieldNo].size());
		int matchs = 0;
		while (leftColumns != null && matchs < vectorsize) {
			List<Object> hashTable = Arrays.asList(leftColumns[leftFieldNo].getAsObject());
			while (rightIdx < rightColumns[rightFieldNo].size() && matchs < vectorsize) {
				if (hashTable.contains(rightColumns[rightFieldNo].get(rightIdx))) {
					matchs++;
					for (int i = 0; i < leftColumns.length; i++) {
						res[i].add(leftColumns[i].get(leftIdx));
					}
					for (int i = leftColumns.length; i < leftColumns.length + rightColumns.length; i++) {
						res[i].add(leftColumns[i].get(rightIdx));
					}
					rightIdx++;
				}
			}
			if (matchs < vectorsize) {
				leftColumns = leftChild.next();
				leftIdx++;
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
}
