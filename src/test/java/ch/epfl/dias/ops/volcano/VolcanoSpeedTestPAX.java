package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.PAX.PAXStore;
import ch.epfl.dias.store.row.DBTuple;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

public class VolcanoSpeedTestPAX {

	DataType[] orderSchema;
	DataType[] lineitemSchema;
	DataType[] schema;

	PAXStore paxstoreData;
	PAXStore paxstoreOrder;
	PAXStore paxstoreLineItem;
	int standardVectorsize = 3;

	@Before
	public void init() throws IOException {

		schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT };

		orderSchema = new DataType[] { DataType.INT, DataType.INT, DataType.STRING, DataType.DOUBLE, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.INT, DataType.STRING };

		lineitemSchema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE, DataType.STRING, DataType.STRING, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING };

		paxstoreData = new PAXStore(schema, "input/data.csv", ",", standardVectorsize);
		paxstoreData.load();

		paxstoreOrder = new PAXStore(orderSchema, "input/orders_big.csv", "\\|", standardVectorsize);
		paxstoreOrder.load();

		paxstoreLineItem = new PAXStore(lineitemSchema, "input/lineitem_big.csv", "\\|", standardVectorsize);
		paxstoreLineItem.load();
	}

	private long test1() {
		long start = System.currentTimeMillis();
		/*
		 * SELECT l_partkey, l_comment FROM lineitem WHERE l_quantity > 20
		 */

		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(paxstoreLineItem);

		/* Filtering on both sides */
		ch.epfl.dias.ops.volcano.Select selLineitem = new ch.epfl.dias.ops.volcano.Select(scanLineitem, BinaryOp.LT, 4,
				20);
		int[] columns = new int[] { 1, 15 };
		ch.epfl.dias.ops.volcano.Project projectLineitem = new ch.epfl.dias.ops.volcano.Project(selLineitem, columns);

		ArrayList<DBTuple> result = new ArrayList<DBTuple>();
		projectLineitem.open();
		DBTuple current = projectLineitem.next();
		while (!current.eof) {
			result.add(current);
			current = projectLineitem.next();
		}
		projectLineitem.close();
		long finish = System.currentTimeMillis();
		System.out.println(result.size());
		return finish - start;
	}

	private long test2() {
		long start = System.currentTimeMillis();
		/*
		 * SELECT FROM l_comment, o_comment JOIN lineitem ON (o_orderkey = l_orderkey)
		 * WHERE orderkey = 3;
		 */

		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(paxstoreLineItem);
		ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(paxstoreOrder);

		/* Filtering on both sides */
		ch.epfl.dias.ops.volcano.HashJoin join = new ch.epfl.dias.ops.volcano.HashJoin(scanLineitem, scanOrder, 0, 0);
		ch.epfl.dias.ops.volcano.Project project = new ch.epfl.dias.ops.volcano.Project(join, new int[] { 15, 23 });

		ArrayList<DBTuple> result = new ArrayList<DBTuple>();
		project.open();
		DBTuple current = project.next();
		while (!current.eof) {
			result.add(current);
			current = project.next();
		}
		project.close();
		long finish = System.currentTimeMillis();
		System.out.println(result.size());
		return finish - start;
	}

	private long test3() {
		long start = System.currentTimeMillis();
		/*
		 * SELECT AVG(l_quantity) FROM lineitem WHERE l_quantity > 10
		 */

		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(paxstoreLineItem);

		/* Filtering */
		ch.epfl.dias.ops.volcano.Select selLineitem = new ch.epfl.dias.ops.volcano.Select(scanLineitem, BinaryOp.LT, 4,
				10);

		ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(selLineitem,
				Aggregate.AVG, DataType.INT, 4);
		agg.open();
		DBTuple result = agg.next();
		agg.close();
		long finish = System.currentTimeMillis();

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

		System.out.println("volcano store takes on average " + runningTimes[0] / 5000.0 + "s for query1, "
				+ runningTimes[1] / 5000.0 + "s for query2 and " + runningTimes[2] / 5000.0 + "s for query3");
	}
}