package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

public class ColumnarSpeedTest {

	DataType[] orderSchema;
	DataType[] lineitemSchema;
	DataType[] schema;

	ColumnStore columnstoreData;
	ColumnStore columnstoreOrder;
	ColumnStore columnstoreLineItem;

	@Before
	public void init() throws IOException {

		schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT };

		orderSchema = new DataType[] { DataType.INT, DataType.INT, DataType.STRING, DataType.DOUBLE, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.INT, DataType.STRING };

		lineitemSchema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE, DataType.STRING, DataType.STRING, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING };

		columnstoreData = new ColumnStore(schema, "input/data.csv", ",");
		columnstoreData.load();

		columnstoreOrder = new ColumnStore(orderSchema, "input/orders_big.csv", "\\|");
		columnstoreOrder.load();

		columnstoreLineItem = new ColumnStore(lineitemSchema, "input/lineitem_big.csv", "\\|");
		columnstoreLineItem.load();
	}

	private long test1() {
		long start = System.currentTimeMillis();
		/*
		 * SELECT l_partkey, l_comment FROM lineitem WHERE l_quantity > 20
		 */

		ch.epfl.dias.ops.columnar.Scan scanLineitem = new ch.epfl.dias.ops.columnar.Scan(columnstoreLineItem);

		/* Filtering on both sides */
		ch.epfl.dias.ops.columnar.Select selLineitem = new ch.epfl.dias.ops.columnar.Select(scanLineitem, BinaryOp.LT,
				4, 20);
		int[] columns = new int[] { 1, 15 };
		ch.epfl.dias.ops.columnar.Project projectLineitem = new ch.epfl.dias.ops.columnar.Project(selLineitem, columns);
		
		DBColumn[] result = projectLineitem.execute();

		long finish = System.currentTimeMillis();
		System.out.println(result[0].size());
		return finish - start;
	}

	private long test2() {
		long start = System.currentTimeMillis();
		/*
		 * SELECT FROM l_comment, o_comment JOIN lineitem ON (o_orderkey = l_orderkey)
		 * WHERE orderkey = 3;
		 */

		ch.epfl.dias.ops.columnar.Scan scanLineitem = new ch.epfl.dias.ops.columnar.Scan(columnstoreLineItem);
		ch.epfl.dias.ops.columnar.Scan scanOrder = new ch.epfl.dias.ops.columnar.Scan(columnstoreOrder);

		/* Filtering on both sides */
		ch.epfl.dias.ops.columnar.Join join = new ch.epfl.dias.ops.columnar.Join(scanLineitem, scanOrder, 0, 0);
		ch.epfl.dias.ops.columnar.Project project = new ch.epfl.dias.ops.columnar.Project(join, new int[] { 15, 23 });

		DBColumn[] result = project.execute();

		long finish = System.currentTimeMillis();
		System.out.println(result[0].size());
		return finish - start;
	}

	private long test3() {
		long start = System.currentTimeMillis();
		/*
		 * SELECT AVG(l_quantity) FROM lineitem WHERE l_quantity > 10
		 */

		ch.epfl.dias.ops.columnar.Scan scanLineitem = new ch.epfl.dias.ops.columnar.Scan(columnstoreLineItem);

		/* Filtering */
		ch.epfl.dias.ops.columnar.Select selLineitem = new ch.epfl.dias.ops.columnar.Select(scanLineitem, BinaryOp.LT,
				4, 10);

		ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(selLineitem,
				Aggregate.AVG, DataType.INT, 4);

		DBColumn[] result = agg.execute();

		long finish = System.currentTimeMillis();
		System.out.println(result[0].size());
		return finish - start;
	}

	@Test
	public void testAll() {
		long[] runningTimes = new long[] { 0, 0, 0 };

		for (int i = 0; i < 5; i++) {
			runningTimes[0] = runningTimes[0] + test1();
			runningTimes[1] = runningTimes[1] + test2();
			runningTimes[2] = runningTimes[2] + test3();
		}

		System.out.println("Columnar store takes on average " + runningTimes[0] / 5000.0 + "s for query1, "
				+ runningTimes[1] / 5000.0 + "s for query2 and " + runningTimes[2] / 5000.0 + "s for query3");
	}
}