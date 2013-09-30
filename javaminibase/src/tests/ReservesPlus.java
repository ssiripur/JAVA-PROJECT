package tests;

// Define the Reserves schema
class ReservesPlus {
	public int sid;
	public int bid;
	public String date;
	public float score;

	public ReservesPlus(int _sid, int _bid, String _date, float score) {
		sid = _sid;
		bid = _bid;
		date = _date;
		this.score=score;

	}
}