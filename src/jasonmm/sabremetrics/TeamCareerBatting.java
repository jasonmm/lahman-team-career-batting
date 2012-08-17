package jasonmm.sabremetrics;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;


/**
 * Processes a given team.
 */
class TeamProcessor implements Runnable {
	protected String teamId = "";
	protected int yearId = 0;
	protected HashMap<String, Integer> teamTotals = null;
	
	TeamProcessor(String t, int y) {
		this.teamId = t;
		this.yearId = y;
	}

	/**
	 * Sets all the batting statistics in the hash map to zero.
	 */
	protected void initTeamTotals() {
		this.teamTotals.put("AB", 0);
		this.teamTotals.put("R", 0);
		this.teamTotals.put("H", 0);
		this.teamTotals.put("2B", 0);
		this.teamTotals.put("3B", 0);
		this.teamTotals.put("HR", 0);
		this.teamTotals.put("RBI", 0);
		this.teamTotals.put("SB", 0);
		this.teamTotals.put("CS", 0);
		this.teamTotals.put("BB", 0);
		this.teamTotals.put("SO", 0);
		this.teamTotals.put("IBB", 0);
		this.teamTotals.put("HBP", 0);
		this.teamTotals.put("SH", 0);
		this.teamTotals.put("SF", 0);
		this.teamTotals.put("GIDP", 0);
	}
	
	/**
	 * Increase the teamTotal value of "stat" by "amt".
	 * @param stat
	 * @param amt
	 */
	protected void incTeamTotal(String stat, int amt) {
		this.teamTotals.put(stat, (this.teamTotals.get(stat)+amt));
	}
	
	/**
	 * Writes the teamTotals hash map to the database.
	 * @param conn
	 */
	protected void saveTeamTotals(Connection conn) {
		PreparedStatement pstmt = null;
		int singles = this.teamTotals.get("H")-this.teamTotals.get("2B")-this.teamTotals.get("3B")-this.teamTotals.get("HR");
		int sluggingNumerator = 
				singles+
				(2*this.teamTotals.get("2B"))+
				(3*this.teamTotals.get("3B"))+
				(4*this.teamTotals.get("HR"))
		;

		try {
			pstmt = conn.prepareStatement("REPLACE INTO TeamCareerBatting VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			pstmt.setInt(1, this.yearId);
			pstmt.setString(2, this.teamId);
			pstmt.setString(3, "");
			pstmt.setInt(4, this.teamTotals.get("AB"));
			pstmt.setInt(5, this.teamTotals.get("R"));
			pstmt.setInt(6, this.teamTotals.get("H"));
			pstmt.setInt(7, this.teamTotals.get("2B"));
			pstmt.setInt(8, this.teamTotals.get("3B"));
			pstmt.setInt(9, this.teamTotals.get("HR"));
			pstmt.setInt(10, this.teamTotals.get("RBI"));
			pstmt.setInt(11, this.teamTotals.get("SB"));
			pstmt.setInt(12, this.teamTotals.get("CS"));
			pstmt.setInt(13, this.teamTotals.get("BB"));
			pstmt.setInt(14, this.teamTotals.get("SO"));
			pstmt.setInt(15, this.teamTotals.get("IBB"));
			pstmt.setInt(16, this.teamTotals.get("HBP"));
			pstmt.setInt(17, this.teamTotals.get("SH"));
			pstmt.setInt(18, this.teamTotals.get("SF"));
			pstmt.setInt(19, this.teamTotals.get("GIDP"));
			if( this.teamTotals.get("AB") > 0 ) {
				pstmt.setDouble(20, (double)this.teamTotals.get("H")/(double)this.teamTotals.get("AB"));
				pstmt.setDouble(21, (double)this.teamTotals.get("H")/(double)(this.teamTotals.get("AB")+this.teamTotals.get("BB")));
				pstmt.setDouble(22, (double)sluggingNumerator/(double)this.teamTotals.get("AB"));
			}
			else {
				pstmt.setDouble(20, 0.00);
				pstmt.setDouble(21, 0.00);
				pstmt.setDouble(22, 0.00);
			}
			pstmt.executeUpdate();
		} catch( SQLException e ) {
			e.printStackTrace();
		} finally {
			try {
				if( pstmt != null ) {
					pstmt.close();
				}
			} catch( SQLException e ) {}
		}
	}
	
	public void run() {
		Connection conn = null;
		PreparedStatement pstmt = null;
		PreparedStatement pstmtPlayer = null;
		ResultSet rsPlayers = null;
		ResultSet rsPlayerCareer = null;
		
		try {
			conn = TeamCareerBatting.connectToDatabase();

			this.teamTotals = new HashMap<String, Integer>(16);
			this.initTeamTotals();
			
			// Get the IDs of every player who played on the given team.
			// Also only get players from teams that are not already in 
			// the TeamCareerBatting table.
			pstmt = conn.prepareStatement(
					"SELECT b.playerID "+
					"FROM Batting b LEFT JOIN TeamCareerBatting tcb ON (b.teamID=tcb.teamID AND b.yearID=tcb.yearID) "+
					"WHERE b.teamID=? AND b.yearID=? AND tcb.AB IS NULL"
			);
			pstmt.setString(1, this.teamId);
			pstmt.setInt(2, this.yearId);
			rsPlayers = pstmt.executeQuery();

			if( rsPlayers.next() ) {
				do {
					pstmtPlayer = conn.prepareStatement(
						"SELECT "+
						" COALESCE(SUM(AB), 0) AS cAB, "+
						" COALESCE(SUM(R), 0) AS cR, "+
						" COALESCE(SUM(H), 0) AS cH, "+
						" COALESCE(SUM(2B), 0) AS c2B, "+
						" COALESCE(SUM(3B), 0) AS c3B, "+
						" COALESCE(SUM(HR), 0) AS cHR, "+
						" COALESCE(SUM(RBI), 0) AS cRBI, "+
						" COALESCE(SUM(SB), 0) AS cSB, "+
						" COALESCE(SUM(CS), 0) AS cCS, "+
						" COALESCE(SUM(BB), 0) AS cBB, "+
						" COALESCE(SUM(SO), 0) AS cSO, "+
						" COALESCE(SUM(IBB), 0) AS cIBB, "+
						" COALESCE(SUM(HBP), 0) AS cHBP, "+
						" COALESCE(SUM(SH), 0) AS cSH, "+
						" COALESCE(SUM(SF), 0) AS cSF, "+
						" COALESCE(SUM(GIDP), 0) AS cGIDP "+
						"FROM Batting "+
						"WHERE playerID=? AND yearID<? "
					);
					pstmtPlayer.setString(1, rsPlayers.getString("playerID"));
					pstmtPlayer.setInt(2, this.yearId);
					rsPlayerCareer = pstmtPlayer.executeQuery();
					rsPlayerCareer.next();
					this.incTeamTotal("AB", rsPlayerCareer.getInt("cAB"));
					this.incTeamTotal("R", rsPlayerCareer.getInt("cR"));
					this.incTeamTotal("H", rsPlayerCareer.getInt("cH"));
					this.incTeamTotal("2B", rsPlayerCareer.getInt("c2B"));
					this.incTeamTotal("3B", rsPlayerCareer.getInt("c3B"));
					this.incTeamTotal("HR", rsPlayerCareer.getInt("cHR"));
					this.incTeamTotal("RBI", rsPlayerCareer.getInt("cRBI"));
					this.incTeamTotal("SB", rsPlayerCareer.getInt("cSB"));
					this.incTeamTotal("CS", rsPlayerCareer.getInt("cCS"));
					this.incTeamTotal("BB", rsPlayerCareer.getInt("cBB"));
					this.incTeamTotal("SO", rsPlayerCareer.getInt("cSO"));
					this.incTeamTotal("IBB", rsPlayerCareer.getInt("cIBB"));
					this.incTeamTotal("HBP", rsPlayerCareer.getInt("cHBP"));
					this.incTeamTotal("SH", rsPlayerCareer.getInt("cSH"));
					this.incTeamTotal("SF", rsPlayerCareer.getInt("cSF"));
					this.incTeamTotal("GIDP", rsPlayerCareer.getInt("cGIDP"));
				} while( rsPlayers.next() );
				
				this.saveTeamTotals(conn);
			}
		} catch( SQLException e ) {
			e.printStackTrace();
		} finally {
			try {
				if( conn != null ) {
					conn.close();
				}
				if( pstmt != null ) {
					pstmt.close();
				}
				if( pstmtPlayer != null ) {
					pstmtPlayer.close();
				}
				if( rsPlayers != null ) {
					rsPlayers.close();
				}
				if( rsPlayerCareer != null ) {
					rsPlayerCareer.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}



public class TeamCareerBatting {
	public static final String DATABASE = "lahman591";
	public static final String USERNAME = "root";
	public static final String PASSWORD = "jmizher-one";
	public static final int NUM_THREADS = 4;
	
	public static Connection connectToDatabase() {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			return( DriverManager.getConnection("jdbc:mysql://localhost/"+TeamCareerBatting.DATABASE+"?user="+TeamCareerBatting.USERNAME+"&password="+TeamCareerBatting.PASSWORD) );
		} catch (InstantiationException e1) {
			e1.printStackTrace();
		} catch (IllegalAccessException e1) {
			e1.printStackTrace();
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return(null);
	}
	
	protected void start() {
		Connection conn = null;
		ResultSet rsTeams = null;
		ExecutorService svc = Executors.newFixedThreadPool(TeamCareerBatting.NUM_THREADS);
		String teamId = "";
		int yearId = 0;
		
		try {
			System.out.println((new GregorianCalendar()).getTimeInMillis());

			conn = TeamCareerBatting.connectToDatabase();

			// Get all teams for every year.
			rsTeams = conn.createStatement().executeQuery("SELECT teamID, yearID FROM Teams ORDER BY yearID ASC");

			// Loop over every team starting an executor.
			while( rsTeams.next() ) {
				teamId = rsTeams.getString("teamID");
				yearId = rsTeams.getInt("yearID");
				svc.execute(new TeamProcessor(teamId, yearId));
			}
			
			svc.shutdown();
			while( !svc.isTerminated() ) {}

			System.out.println((new GregorianCalendar()).getTimeInMillis());

		} catch( SQLException e ) {
			e.printStackTrace();
		} finally {
			try {
				if( conn != null ) {
					conn.close();
				}
				if( rsTeams != null ) {
					rsTeams.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		(new TeamCareerBatting()).start();
	}

}


