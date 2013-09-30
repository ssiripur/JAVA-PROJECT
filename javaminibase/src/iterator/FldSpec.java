package iterator;

import heap.*;

public class FldSpec {
	public RelSpec relation;
	public int offset;

	public int tableIndex;

	/**
	 * 
	 * 
	 * @param _relation
	 *            the relation is outer or inner
	 * @param _offset
	 *            the offset of the field
	 */
	public FldSpec(RelSpec _relation, int _offset) {
		this(_relation, _offset, -1);
	}

	public FldSpec(RelSpec _relation, int _offset, int _tableIndex) {
		relation = _relation;
		offset = _offset;
		tableIndex = _tableIndex;

	}
}
