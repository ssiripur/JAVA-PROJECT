package tests;

// Define the Reserves schema
class Reserves {
	public int sid;
	public int bid;
	public String date;
	public float score;

	public Reserves(int _sid, int _bid, String _date, float score) {
		sid = _sid;
		bid = _bid;
		date = _date;
		this.score=score;

	}
}