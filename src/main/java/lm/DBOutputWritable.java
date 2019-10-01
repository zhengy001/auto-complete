package lm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DBOutputWritable implements Writable, DBWritable {
	private String starting_phrase;
	private String following_word;
	private int cnt;
	
	
	public DBOutputWritable() {
		super();
	}

	public DBOutputWritable(String starting_phrase, String following_word, int cnt) {
		this.starting_phrase = starting_phrase;
		this.following_word = following_word;
		this.cnt = cnt;
	}

	public void write(PreparedStatement statement) throws SQLException {
		int index = 1;
		
		statement.setString(index++, starting_phrase);
		statement.setString(index++, following_word);
		statement.setInt(index++, cnt);
		
	}
	
	public void readFields(ResultSet rs) throws SQLException {
		int index = 1;
		
		starting_phrase = rs.getString(index++);
		following_word = rs.getString(index++);
		cnt = rs.getInt(index++);
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(starting_phrase);
		out.writeUTF(following_word);
		out.writeInt(cnt);
		
	}

	public void readFields(DataInput in) throws IOException {
		starting_phrase = in.readUTF();
		following_word = in.readUTF();
		cnt = in.readInt();
	}

}
