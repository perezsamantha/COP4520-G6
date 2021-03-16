import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

public class MySQLAccess {
	
  private Connection connect = null;
  private Statement statement = null;
  private PreparedStatement preparedStatement = null;
  private ResultSet resultSet = null;

  final private String host = "";
  final private String user = "";
  final private String password = "";
  
  public void readDataBase() throws Exception {
    try {
      
      //Load the MySQL driver
      Class.forName("com.mysql.jdbc.Driver");
      
      //Connect to DB
      connect = DriverManager
          .getConnection("jdbc:mysql://" + host + "/test"
              + "user=" + user + "&password=" + password );

      // Issue SQL queries to the database
      statement = connect.createStatement();
      
      // Get the result of the SQL query
      resultSet = statement
          .executeQuery("select * from table1");
      writeResultSet(resultSet);
      
    } catch (Exception e) {
      throw e;
    } finally {
      close();
    }

  }

  private void writeMetaData(ResultSet resultSet) throws SQLException {
    // Get the result of the SQL query
    
    System.out.println("The columns in the table are: ");
    
    System.out.println("Table: " + resultSet.getMetaData().getTableName(1));
    for  (int i = 1; i<= resultSet.getMetaData().getColumnCount(); i++){
      System.out.println("Column " +i  + " "+ resultSet.getMetaData().getColumnName(i));
    }
  }

  private void writeResultSet(ResultSet resultSet) throws SQLException {
    while (resultSet.next()) {
      String user = resultSet.getString("myuser");

      String website = resultSet.getString("webpage");

      String date = resultSet.getString("date");
      
      System.out.println("User: " + user);
      System.out.println("Website: " + website);
      System.out.println("Date: " + date);
    }
  }

  //Close the resultSet
  private void close() {
    try {
      if (resultSet != null) {
        resultSet.close();
      }

      if (statement != null) {
        statement.close();
      }

      if (connect != null) {
        connect.close();
      }
    } catch (Exception e) {

    }
  }

}