package tests;

import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import global.RID;
import global.SystemDefs;
import global.TupleOrder;
import heap.Heapfile;
import heap.Tuple;
import iterator.CondExpr;
import iterator.DuplElim;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.RelSpec;
import iterator.Sort;

import java.io.IOException;

import btree.BTreeFile;

public class FuzzyTest extends TestDriver {

	public FuzzyTest() {
		super("FuzzyTest");
	}

//	public void testForDuplicates() {
//
//		AttrType[] attrType = new AttrType[1];
//		attrType[0] = new AttrType(AttrType.attrString);
//		short[] attrSize = new short[1];
//		attrSize[0] = 100;
//
//		// create a tuple of appropriate size
//		Tuple t = new Tuple();
//		try {
//			t.setHdr((short) 1, attrType, attrSize);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//		int size = t.size();
//
//		// Create unsorted data file "test1.in"
//		RID rid;
//		Heapfile f = null;
//		try {
//			f = new Heapfile("testdups.in");
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//		t = new Tuple(size);
//		try {
//			t.setHdr((short) 1, attrType, attrSize);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//		String[] data1 = { "One", "Four", "One", "Three", "One", "Three",
//				"Four", "Four", "Seven" };
//
//		for (int i = 0; i < 9; i++) {
//			try {
//				t.setStrFld(1, data1[i]);
//				float score = (float) Math.random();
//				t.setScore(score);
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//
//			try {
//				byte[] retArray = t.returnTupleByteArray();
//				rid = f.insertRecord(t.returnTupleByteArray());
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//
//		// create an iterator by open a file scan
//		FldSpec[] projlist = new FldSpec[1];
//		RelSpec rel = new RelSpec(RelSpec.outer);
//		projlist[0] = new FldSpec(rel, 1);
//
//		FileScan fscan = null;
//		FileScan dupscan = null;
//
//		try {
//			fscan = new FileScan("testdups.in", attrType, attrSize, (short) 1,
//					1, projlist, null);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//		try {
//			Tuple tuple = fscan.get_next();
//			while (tuple != null) {
//				tuple = fscan.get_next();
//			}
//
//			System.out.println("===================");
//
//			try {
//				dupscan = new FileScan("testdups.in", attrType, attrSize,
//						(short) 1, 1, projlist, null);
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//
//			// //Eliminate Dups.
//			// DuplElim dupElimIterator=new DuplElim(attrType, (short)1,
//			// attrSize, dupscan, 10, false);
//			// Tuple dupTuple = dupElimIterator.get_next();
//			// while (dupTuple != null) {
//			// System.out.println(dupTuple.getStrFld(1)+" -- "+dupTuple.getScore());
//			// dupTuple = dupElimIterator.get_next();
//			// }
//
//			Iterator _am = null;
//			TupleOrder order = new TupleOrder(TupleOrder.Ascending);
//			try {
//				_am = new Sort(attrType, (short) 1, attrSize, dupscan, order,
//						10, true);
//			} catch (Exception e) {
//				// TODO: handle exception
//				e.printStackTrace();
//			}
//
//			System.out.println("--------------------");
//
//			try {
//				Tuple tup = null;
//				while ((tup = _am.get_next()) != null) {
//					System.out.println(tup.getStrFld(1) + " -> "
//							+ tup.getScore());
//				}
//			} catch (Exception e) {
//				// TODO: handle exception
//				e.printStackTrace();
//			}
//
//		} catch (Exception e) {
//			e.printStackTrace();
//
//		}
//
//	}

	public boolean runAllTests() {
		System.out
				.println("\n" + "Running " + testName() + " tests...." + "\n");

		SystemDefs sysdef = new SystemDefs(dbpath, 300, 50, "Clock");

		// Kill anything that might be hanging around
		String newdbpath;
		String newlogpath;
		String remove_logcmd;
		String remove_dbcmd;
		String remove_cmd = "/bin/rm -rf ";

		newdbpath = dbpath;
		newlogpath = logpath;

		remove_logcmd = remove_cmd + logpath;
		remove_dbcmd = remove_cmd + dbpath;

		// Commands here is very machine dependent. We assume
		// user are on UNIX system here
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		} catch (IOException e) {
			System.err.println("" + e);
		}

		remove_logcmd = remove_cmd + newlogpath;
		remove_dbcmd = remove_cmd + newdbpath;

		// This step seems redundant for me. But it's in the original
		// C++ code. So I am keeping it as of now, just in case I
		// I missed something
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		} catch (IOException e) {
			System.err.println("" + e);
		}

		// Run the tests. Return type different from C++
		//testForDuplicates();

		testNestedLoopJoin();
		// Clean up again
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		} catch (IOException e) {
			System.err.println("" + e);
		}
		return false;

	}

	public void testNestedLoopJoin() {

		AttrType[] attrTypeA = new AttrType[3];
		attrTypeA[0] = new AttrType(AttrType.attrInteger);
		attrTypeA[1] = new AttrType(AttrType.attrString);
		attrTypeA[2] = new AttrType(AttrType.attrInteger);

		AttrType[] attrTypeB = new AttrType[2];
		attrTypeB[0] = new AttrType(AttrType.attrInteger);
		attrTypeB[1] = new AttrType(AttrType.attrString);

		short[] attrSizeA = new short[3];
		attrSizeA[0] = 4;
		attrSizeA[1] = 4;
		attrSizeA[2] = 100;

		short[] attrSizeB = new short[2];
		attrSizeB[0] = 4;
		attrSizeB[1] = 100;

		// create a tuple of appropriate size
		Tuple ta = new Tuple();
		try {
			ta.setHdr((short) 1, attrTypeA, attrSizeA);
		} catch (Exception e) {
			e.printStackTrace();
		}

		Tuple tb = new Tuple();
		try {
			tb.setHdr((short) 1, attrTypeB, attrSizeB);
		} catch (Exception e) {
			e.printStackTrace();
		}

		int sizeA = ta.size();
		int sizeB = tb.size();

		IndexType b_index = new IndexType(IndexType.B_Index);

		CondExpr[] outFilter = new CondExpr[2];
		outFilter[0] = new CondExpr();
		outFilter[1] = new CondExpr();

		outFilter[0].next = null;
		outFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
		outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
		outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),
				1);
		outFilter[0].operand2.symbol = new FldSpec(
				new RelSpec(RelSpec.innerRel), 1);

		outFilter[1] = null;

		AttrType[] etypes = { new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrInteger) };

		AttrType[] dtypes = { new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrString) };

		// create the index file
		BTreeFile btf = null;
		try {
			btf = new BTreeFile("BTreeIndex", AttrType.attrInteger, 4, 1);
		} catch (Exception e) {
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		// Create unsorted data file "test1.in"
		RID rid;
		Heapfile fA = null;
		Heapfile fB = null;

		try {
			fA = new Heapfile("employee.in");
			fB = new Heapfile("employee.in");
		} catch (Exception e) {
			e.printStackTrace();
		}

		int[] employeeId = { 1, 2, 3, 4, 5, 6 };
		String[] employee = { "Arun", "Bhavya", "Prashant", "Nayana", "Savi",
				"Candan" };
		int[] employeeDeptId = { 1, 2, 2, 1, 1, 3 };

		int[] deptId = { 1, 2, 3 };
		String[] dept = { "DeptA", "DeptB", "DeptC" };

		for (int i = 0; i < 6; i++) {
			try {
				ta.setIntFld(1, employeeId[i]);
				ta.setStrFld(2, employee[i]);
				ta.setIntFld(3, employeeDeptId[i]);
				if (i % 2 == 0) {
					tb.setIntFld(1, deptId[i / 2]);
					tb.setStrFld(2, dept[i / 2]);
				}
				float score = (float) Math.random();
				ta.setScore(score);
				tb.setScore(score / 7);

			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		// Populate index.

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		new FuzzyTest().runAllTests();
	}

}
