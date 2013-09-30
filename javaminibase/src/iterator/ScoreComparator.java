package iterator;

import java.util.Comparator;
import java.util.Map;

public class ScoreComparator<T> implements Comparator<T> {

	private Map<T, Float> base;

	public ScoreComparator(Map<T, Float> base) {
		this.base = base;
	}

	@Override
	public int compare(T o1, T o2) {
		if (base.get(o1) >= base.get(o2)) {
			return -1;
		} else {
			return 1;
		} // returning 0 would merge keys
	}

}
