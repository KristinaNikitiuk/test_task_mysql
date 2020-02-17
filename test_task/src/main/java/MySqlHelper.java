import com.healthmarketscience.sqlbuilder.CreateTableQuery;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSchema;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSpec;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbTable;
import com.oracle.javafx.jmx.json.JSONException;
import org.json.simple.JSONObject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


class MySqlHelper {

    private static Statement stmtObj;

    private static DbSchema schemaObj;

    private static Connection connection = null;

    private static JSONObject config = null;

    /**
     * @throws SQLException
     *
     * !! created config file not to write password directly
     *
     */
    MySqlHelper() throws SQLException {
        config = Utils.parseConfigurations("/workspace/test_task/src/data/config.json");
        String myUrl = "jdbc:mysql://localhost/usersdata?";
        Properties props = new Properties();
        props.setProperty("user", config.get("user").toString());
        props.setProperty("password", config.get("password").toString());
        connection = DriverManager.getConnection(myUrl, props);
        stmtObj = connection.createStatement();

    }

    /**
     * @throws SQLException
     *
     * configuration from the files:
     *         (name of the file =  table name
     *         header = columns names)
     *

     */
    static void connectToMySql() throws SQLException {

        List<String> files = Arrays.asList(config.get("user_path").toString(),
                config.get("city_path").toString(), config.get("country_path").toString());

        try {
            DbSpec specficationObj = new DbSpec();
            schemaObj = specficationObj.addDefaultSchema();

            files.forEach((temp) -> {
                StringBuilder data = Utils.read_csv(temp);
                List<String> header = Utils.read_csv_header(temp);
                String tableName = Utils.getFileName(temp);
                createDbTable(tableName, header);
                insertData(tableName, header, data);
            });

            joinTables();

        } catch (JSONException e) {
            e.printStackTrace();
        } finally {
            if(connection != null) {
                connection.close();
            }
        }
    }


    private static void createDbTable(String tableName, List<String> header) {

        try {
            if (isCreated(tableName)) System.out.println("Table " + tableName + " already exists! \n");
                else {
                    try {
                        DbTable table_name = schemaObj.addTable(tableName);
                        /**
                         * FIRST solution for data type classification!
                         */

                        DataTypeClassificator bc = new DataTypeClassificator();
                        List<String> columnTypes = bc.bayesClassification(header);
                        System.out.println(columnTypes);

                        /**
                         * SECOND approach for data type classification!
                         */

//                        List<String> columnTypes = new ArrayList<>();
//                        for (String aHeader : header) {
//                            columnTypes.add(Utils.getColumnDataType(aHeader));
//                        }
//                        System.out.println(columnTypes);


                        int i = 0;
                        while (i < header.size()) {
                            if (columnTypes.get(i).equals("DATE")) {
                                table_name.addColumn(header.get(i), columnTypes.get(i), null);
                            } else
                                table_name.addColumn(header.get(i), columnTypes.get(i), 250);
                                i++;
                        }

                        String createTableQuery = new CreateTableQuery(table_name, true).validate().toString();
                        System.out.println("\n createTableQuery " + createTableQuery + "\n");
                        stmtObj.execute(createTableQuery);

                        } catch (Exception sqlException) {
                            sqlException.printStackTrace();
                        }
                    System.out.println("\n was created table '" + tableName + "'\n");

                }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    private static void insertData(String tableName, List<String> header, StringBuilder data) {

        String query = "insert into " + tableName + " (" + header + ") values " +
                data.toString().replaceAll(",$", "") + ";";
        executeQuery(query.replace("[", "").replace("]",""), 0);

    }

    private static void joinTables() {

        try {
            if (isCreated("result_table")) System.out.println("Table result_table already exists! \n");
            else {
                String joinQuery = "CREATE TABLE result_table Select user.id, user.name,  user.date_of_birthday, city, country " +
                                "FROM user INNER JOIN (Select city.id as city_id , city.name as city, country.name as country FROM city " +
                                "INNER JOIN country ON city.country_id = country.id) temp ON user.city_id = temp.city_id;";
                executeQuery(joinQuery, 0);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        String showResQuery = "SELECT * FROM result_table;";
        executeQuery(showResQuery, 1);

    }

    /**
     * @param query
     * @param queryType 0 - update query;
     *                  1 - execute query and show result;
     */
    private static void executeQuery(String query, int queryType) {

        try {
            switch(queryType){
                case 0:
                    stmtObj.executeUpdate(query);
                    break;
                case 1:
                    ResultSet rs = stmtObj.executeQuery(query);
                    ResultSetMetaData rsmd = rs.getMetaData();
                    int columnsNumber = rsmd.getColumnCount();

                    while (rs.next()) {
                        for (int i = 1; i <= columnsNumber; i++) {
                            if (i > 1) System.out.print(",");
                            String columnValue = rs.getString(i);
                            System.out.print(columnValue);
                        }
                        System.out.println('\n');
                    }
                    break;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static boolean isCreated(String tableName) throws SQLException {
            DatabaseMetaData dbmd = connection.getMetaData();
            String[] types = {"TABLE"};
            StringBuilder tables = new StringBuilder();
            ResultSet rs = dbmd.getTables(null, null, "%", types);
            while (rs.next()) {
                tables.append(rs.getString("TABLE_NAME"));
            }
            return tables.toString().contains(tableName);
        }

}
