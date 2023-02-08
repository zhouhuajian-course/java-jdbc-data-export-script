
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;

public class OracleDataExportScript01 {
    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        Class.forName("oracle.jdbc.driver.OracleDriver");
        Connection connection;
        connection = DriverManager.getConnection("jdbc:oracle:thin:@//127.0.0.1:1521/orcl",
                                                 "user",
                                                 "password");
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM table_01 WHERE id > ? and ROWNUM <= 1000 ORDER BY id ASC");

        int maxId = 0;
        File file = new File("data.txt");
        FileWriter fileWriter = new FileWriter(file);
        while (true) {
            preparedStatement.setInt(1, maxId);
            ResultSet resultSet = preparedStatement.executeQuery();
            int count = 0;
            while (resultSet.next()) {
                count++;
                int id = resultSet.getInt("ID");
                String code = resultSet.getString("CODE");
                maxId = id;
                String line = code + "|0|1\n";
                fileWriter.write(line);
            }
            // break;
            if (count == 0) {
                System.out.println("导出结束");
                break;
            }
        }
        fileWriter.close();
    }
}
