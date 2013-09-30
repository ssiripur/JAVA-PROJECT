package global;

import java.io.IOException;
import java.util.Arrays;

import diskmgr.DB;
//import diskmgr.DBHeaderPage;
import diskmgr.Page;

import bufmgr.BufMgr;
import bufmgr.PageNotReadException;
import index.IndexException;
import iterator.IoBuf;
import iterator.Iterator;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.PredEvalException;
import iterator.SortException;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;
import heap.HFPage;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;

public class performance {

	public static int getTupleSize(Tuple t) {
		// Tuple t1;
		int size = t.getLength();
		return size;
	}

	public static int getNumOfTuples(Iterator i)
			throws CloneNotSupportedException {
		// Iterator _am1;
		// need to duplicate the Iterator
		Iterator _am = (Iterator) i.clone();
		int num_of_tuples = 0;
		try {
			if (_am.get_next() != null)
				num_of_tuples++;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return num_of_tuples;
	}

	public static int getPageSize(Page apage) {
		byte[] data;
		int size_of_page;
		data = apage.getpage();
		size_of_page = data.length;
		return size_of_page;
	}

	public static PageId getCurrentPage(IoBuf ib) {
		PageId p1 = new PageId();
		p1.pid = ib.getCurrentPage();
		/*
		 * //p. //String name=p.db_name(); ib.curr_page; Tuple t=new Tuple(); t.
		 * try { p1=p.get_file_entry(name); }catch(Exception e) {
		 * e.printStackTrace(); }
		 */
		return p1;
	};


	// do not use in-memory count
	public static int getTotalNumOfPages(DB p) {
		int num;
		num = p.db_num_pages();
		return num;
	};

	public static int getBufferSize(BufMgr mgr) {
		int n = mgr.getNumBuffers();
		return n * 1024;
	}

	// percentage of page not free space.
	public static int getPageUtilization(Page apage) {
		byte[] data = null;
		int freespace1 = 0;
		// look up how real data is be kept track of.
		try {
			data = apage.getpage();
			int i = data.length;
			for (int i1 = 0; i1 < i; i1++) {
				if (data[i1] == 0)
					freespace1 += 1;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ((data.length) / GlobalConst.MAX_SPACE) * 100;
	}
}
