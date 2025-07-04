import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class code {


    //  Database info
    static final String DB_URL = "jdbc:mysql://localhost:3306/";

    //static final String USER = "root";
    //static final String PASSWORD = "123456";
    static final String DATA_BASE_NAME = "stream";

    public static void main(String[] args) throws Exception {
        //Student ID:38744503
        // Create the database
        createDatabase();
        createDatabase_1();
        // Create the tables
        createTables();

        // Initialize the Category table
        initializeCategory();

        // Insert game data from a CSV file into the database
        insertGamesIntoDB("38744503.csv");


        // simple Querie
        getAllGamesAndDevelopers();

        // Group By-Having Queries
        getGameCountPerDeveloper();
        getAverageRatingsPerCategory();

        // Deletion Queries
        deleteCategory(1);
        deleteGame(10);

        //student ID:38541548

        // create database first :
       

        // create tables first:
        createTable();

        // now populate data:
        populateData("38541548.csv");

        // constraint 1
        constraint_1();

        //constraint 2
        constraint_2();

        // group having 1
        commonItemTypes();

        // group having 2
        showPopularGames();

    }

    public static void createDatabase() throws SQLException {
        try {
            Connection connection = DriverManager.getConnection(DB_URL);
            Statement statement = connection.createStatement();
            String createDatabaseSql = "CREATE DATABASE IF NOT EXISTS " + DATA_BASE_NAME;
            statement.executeUpdate(createDatabaseSql);
            System.out.println("Successfully created database for 38744503: " + DATA_BASE_NAME);
            } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void createTables() throws SQLException {
        try {
            Connection connection = DriverManager.getConnection(DB_URL + DATA_BASE_NAME);
            Statement statement = connection.createStatement();

            String createCategoryTable = "CREATE TABLE IF NOT EXISTS Category (" +
                    "category_id INT PRIMARY KEY," +
                    "category_name VARCHAR(255)" +
                    ")";
            statement.executeUpdate(createCategoryTable);
            System.out.println("Successfully created table: Category");

            String createGameTable = "CREATE TABLE IF NOT EXISTS Game (" +
                    "game_id INT PRIMARY KEY," +
                    "game_name VARCHAR(255)," +
                    "developer VARCHAR(255)," +
                    "positive_ratings INT," +
                    "negative_ratings INT," +
                    "average_playtime INT," +
                    "price DECIMAL(10, 2)" +
                    ")";
            statement.executeUpdate(createGameTable);
            System.out.println("Successfully created table: Game");

            String createGameCategoryTable = "CREATE TABLE IF NOT EXISTS GameCategory (" +
                    "game_id INT," +
                    "category_id INT," +
                    "PRIMARY KEY (game_id, category_id)," +
                    "FOREIGN KEY (game_id) REFERENCES Game(game_id)," +
                    "FOREIGN KEY (category_id) REFERENCES Category(category_id)" +
                    ")";
            statement.executeUpdate(createGameCategoryTable);
            System.out.println("Successfully created table: GameCategory");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void insertGamesIntoDB(String csvFilePath) throws Exception {
        try (Connection connection = DriverManager.getConnection(DB_URL + DATA_BASE_NAME);
             BufferedReader br = new BufferedReader(new FileReader(csvFilePath));
             PreparedStatement statement = connection.prepareStatement("INSERT INTO Game (game_id, game_name, developer, positive_ratings, negative_ratings, average_playtime, price) VALUES (?, ?, ?, ?, ?, ?, ?)");
             PreparedStatement gameCategoryStatement = connection.prepareStatement("INSERT INTO GameCategory (game_id, category_id) VALUES (?, ?)")) {


            String line;
            br.readLine();
            int gameId = 1;
            while ((line = br.readLine()) != null) {
                String[] data = line.split(",");
                statement.setInt(1, gameId);
                statement.setString(2, data[0]);
                statement.setString(3, data[1]);
                try {
                    statement.setInt(4, Integer.parseInt(data[2]));
                    statement.setInt(5, Integer.parseInt(data[3]));
                    statement.setInt(6, Integer.parseInt(data[4]));
                    statement.setDouble(7, Double.parseDouble(data[5]));
                } catch (NumberFormatException e) {
                    System.err.println("Error parsing data for row: " + line);
                    continue;
                }
                statement.executeUpdate();

                String genresString = data[6];


                Map<String, Integer> CategoryMap = new HashMap<>();
                CategoryMap.put("Multi-player", 1);
                CategoryMap.put("Single and Multi-player", 2);
                CategoryMap.put("Single-player", 3);

                int categoryId = CategoryMap.get(genresString);
                gameCategoryStatement.setInt(1, gameId);
                gameCategoryStatement.setInt(2, categoryId);
                gameCategoryStatement.executeUpdate();
                gameId++;
            }
        } catch (Exception e) {
            System.out.println(e.toString());
        }
        System.out.println("Games inserted successfully.");
    }


    private static void initializeCategory() throws SQLException {
        try (Connection connection = DriverManager.getConnection(DB_URL + DATA_BASE_NAME);
             PreparedStatement statement = connection.prepareStatement("INSERT INTO Category (category_id, category_name) VALUES (?, ?)")) {
            String[] categories = {"Multi-player", "Single and Multi-player", "Single-player"};
            int categoryId = 1;
            for (String categoryName : categories) {
                statement.setInt(1, categoryId++);
                statement.setString(2, categoryName);
                statement.executeUpdate();
            }
        }
        System.out.println("Category table initialized successfully.");
    }

    private static void getAllGamesAndDevelopers() {
        try (Connection connection = DriverManager.getConnection(DB_URL + DATA_BASE_NAME);
             PreparedStatement statement = connection.prepareStatement(
                "SELECT game_name, developer FROM Game")) {
            ResultSet resultSet = statement.executeQuery();

            System.out.println("Game Name\tDeveloper");
            System.out.println("---------------------");
            while (resultSet.next()) {
                String gameName = resultSet.getString("game_name");
                String developer = resultSet.getString("developer");
                System.out.printf("%s\t%s\n", gameName, developer);
            }
        } catch (SQLException e) {
            System.err.println("Error executing 'getAllGamesAndDevelopers' query: " + e.getMessage());
        }
    }

    private static void getGameCountPerDeveloper() {
        try (Connection connection = DriverManager.getConnection(DB_URL + DATA_BASE_NAME);
             PreparedStatement statement = connection.prepareStatement(
                "SELECT developer, COUNT(*) AS game_count " +
                        "FROM Game " +
                        "GROUP BY developer " +
                        "HAVING COUNT(*) >= 2")) {
            ResultSet resultSet = statement.executeQuery();

            System.out.println("Developer\tGame Count");
            System.out.println("-----------------------------");
            while (resultSet.next()) {
                String developer = resultSet.getString("developer");
                int gameCount = resultSet.getInt("game_count");
                System.out.printf("%s\t%d\n", developer, gameCount);
            }
        } catch (SQLException e) {
            System.err.println("Error executing 'getGameCountPerDeveloper' query: " + e.getMessage());
        }
    }

    private static void getAverageRatingsPerCategory() {
        try (Connection connection = DriverManager.getConnection(DB_URL + DATA_BASE_NAME);
             PreparedStatement statement = connection.prepareStatement(
                "SELECT c.category_name, " +
                        "       AVG(g.positive_ratings) AS avg_positive_ratings, " +
                        "       AVG(g.negative_ratings) AS avg_negative_ratings " +
                        "FROM GameCategory gc " +
                        "JOIN Game g ON gc.game_id = g.game_id " +
                        "JOIN Category c ON gc.category_id = c.category_id " +
                        "GROUP BY c.category_name " +
                        "HAVING AVG(g.positive_ratings) > 5")) {
            ResultSet resultSet = statement.executeQuery();

            System.out.println("Category\tAvg Positive Ratings\tAvg Negative Ratings");
            System.out.println("----------------------------------------------------------");
            while (resultSet.next()) {
                String categoryName = resultSet.getString("category_name");
                double avgPositiveRatings = resultSet.getDouble("avg_positive_ratings");
                double avgNegativeRatings = resultSet.getDouble("avg_negative_ratings");
                System.out.printf("%s\t%.2f\t\t%.2f\n", categoryName, avgPositiveRatings, avgNegativeRatings);
            }
        } catch (SQLException e) {
            System.err.println("Error executing 'getAverageRatingsPerCategory' query: " + e.getMessage());
        }
    }

    private static void deleteCategory(int categoryId) {
        try (Connection connection = DriverManager.getConnection(DB_URL + DATA_BASE_NAME);
             PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM Category WHERE category_id = ?")) {
            statement.setInt(1, categoryId);
            int rowsDeleted = statement.executeUpdate();
            System.out.println("Rows deleted from Category table: " + rowsDeleted);
        } catch (SQLException e) {
            System.err.println("Error deleting category with ID " + categoryId + ": " + e.getMessage());
        }
    }

    private static void deleteGame(int gameId) {
        try (Connection connection = DriverManager.getConnection(DB_URL + DATA_BASE_NAME);
             PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM Game WHERE game_id = ?")) {
            statement.setInt(1, gameId);
            int rowsDeleted = statement.executeUpdate();
            System.out.println("Rows deleted from Game table: " + rowsDeleted);
        } catch (SQLException e) {
            System.err.println("Error deleting game with ID " + gameId + ": " + e.getMessage());
        }
    }
    
    // Student ID: 38541548

    /* public static void createDatabase_1() {
        try (Connection conn = DriverManager.getConnection(DB_URL);
             Statement stmt = conn.createStatement()) {

            // Drop the database if it already exists
            String dropSql = "DROP DATABASE IF EXISTS adventure_game";
            stmt.executeUpdate(dropSql);


            String sql = "CREATE DATABASE adventure_game";
            stmt.executeUpdate(sql);

        } catch (SQLException e) {
            System.out.println("Database creation failed.");
            e.printStackTrace();
        }
    }*/
    

    public static void createDatabase_1() throws SQLException {
        try {
            Connection connection = DriverManager.getConnection(DB_URL);
            Statement statement = connection.createStatement();
            String createDatabaseSql = "CREATE DATABASE IF NOT EXISTS adventure_game" ;
            statement.executeUpdate(createDatabaseSql);
            System.out.println("Successfully created database: adventure_game" );
            } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void createTable() {
        String dbUrl = DB_URL + "adventure_game";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             Statement stmt = conn.createStatement()) {

            String createGameTable = "CREATE TABLE game (" +
                    "game_id INT PRIMARY KEY AUTO_INCREMENT, " +
                    "developer VARCHAR(255), " +
                    "release_year INT)";

            String createCharactersTable = "CREATE TABLE characters (" +
                    "character_id INT PRIMARY KEY AUTO_INCREMENT, " +
                    "name VARCHAR(255), " +
                    "health INT, " +
                    "strength INT, " +
                    "game_id INT, " +
                    "FOREIGN KEY (game_id) REFERENCES game(game_id))";

            String createLocationsTable = "CREATE TABLE locations (" +
                    "location_id INT PRIMARY KEY AUTO_INCREMENT, " +
                    "name VARCHAR(255), " +
                    "description TEXT, " +
                    "game_id INT, " +
                    "FOREIGN KEY (game_id) REFERENCES game(game_id))";

            String createItemsTable = "CREATE TABLE items (" +
                    "item_id INT PRIMARY KEY AUTO_INCREMENT, " +
                    "name VARCHAR(255), " +
                    "type VARCHAR(100), " +
                    "effect VARCHAR(255), " +
                    "location_id INT, " +
                    "FOREIGN KEY (location_id) REFERENCES locations(location_id))";


            stmt.execute(createGameTable);
            stmt.execute(createCharactersTable);
            stmt.execute(createLocationsTable);
            stmt.execute(createItemsTable);


            // Alter the game table to add a new column
            stmt.execute("ALTER TABLE game ADD COLUMN title VARCHAR(255)");
            // Create an index on the developer column of the game table
            stmt.execute("CREATE INDEX idx_developer ON game(developer)");

            System.out.println("Database for Student 38541548 has been created ...\n");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void populateData(String csvFile) {
        String dbUrl = DB_URL + "adventure_game";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("game_id")) {

                    populateTableFromCSV(br, conn, "INSERT INTO game (game_id,title, developer, release_year) VALUES (?,?, ?, ?)");
                } else if (line.startsWith("character_id")) {

                    populateTableFromCSV(br, conn, "INSERT INTO characters (character_id,name, health, strength, game_id) VALUES (?,?, ?, ?, ?)");
                } else if (line.startsWith("location_id")) {

                    populateTableFromCSV(br, conn, "INSERT INTO locations (location_id,name, description, game_id) VALUES (?,?, ?, ?)");
                } else if (line.startsWith("item_id")) {

                    populateTableFromCSV(br, conn, "INSERT INTO items (item_id,name, type, effect, location_id) VALUES (?,?, ?, ?, ?)");
                }
            }

            System.out.println("Database for Student 38541548 has been populated ...\n");
        } catch (Exception e) {
            System.out.println("Error populating data from " + csvFile);
            e.printStackTrace();
        }
    }

    private static void populateTableFromCSV(BufferedReader br, Connection conn, String sql) throws Exception {
        PreparedStatement pstmt = conn.prepareStatement(sql);
        String line;
        int count = 0;
        while ((line = br.readLine()) != null && !line.isEmpty()) {

            String[] data = line.split(",", -1);

            int parameterCount = pstmt.getParameterMetaData().getParameterCount();

            for (int i = 0; i < parameterCount; i++) {
                pstmt.setString(i + 1, data[i].trim());

            }
            pstmt.executeUpdate();
            count++;
            if (count == 200) {
                break;
            }
        }
    }


    public static void constraint_1() {
        String dbUrl = DB_URL + "adventure_game";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             Statement stmt = conn.createStatement()) {

            stmt.executeUpdate("DELETE FROM game WHERE game_id = 9");

        } catch (SQLException e) {
            System.out.println("Query 1 failed [Foreign key constraint breached]\n");
        }

    }

    public static void constraint_2() {
        String dbUrl = DB_URL + "adventure_game";
        try (Connection conn = DriverManager.getConnection(dbUrl);
             Statement stmt = conn.createStatement()) {

            stmt.executeUpdate("INSERT INTO locations (location_id, name, description, game_id)" +
                    " VALUES (101, 'New game', 'Description test 2', 999)");


        } catch (SQLException e) {
            System.out.println("Query 2 failed [Foreign key constraint breached]\n");

        }

    }

    public static void showPopularGames() {
        String dbUrl = DB_URL + "adventure_game";
        String query = "SELECT game_id, COUNT(character_id) AS character_count " +
                "FROM characters " +
                "GROUP BY game_id " +
                "HAVING character_count >= 3";

        try (Connection conn = DriverManager.getConnection(dbUrl);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            System.out.println("Games with a high number of characters:\n\n");
            while (rs.next()) {
                int gameId = rs.getInt("game_id");
                int count = rs.getInt("character_count");
                System.out.println("Game ID " + gameId + " has " + count + " characters.");
            }
            System.out.println("\n");
        } catch (SQLException e) {
            System.out.println("Error executing query: showPopularGames");
            e.printStackTrace();
        }
    }

    public static void commonItemTypes() {
        String dbUrl = DB_URL + "adventure_game";
        String query = "SELECT type, COUNT(item_id) AS item_count " +
                "FROM items " +
                "GROUP BY type " +
                "HAVING item_count > 5";

        try (Connection conn = DriverManager.getConnection(dbUrl);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            System.out.println("Most common item types across locations:\n\n");
            while (rs.next()) {
                String type = rs.getString("type");
                int count = rs.getInt("item_count");
                System.out.println(type + " items appear " + count + " times across locations.");
            }
            System.out.println("\n");
        } catch (SQLException e) {
            System.out.println("Error executing query: commonItemTypes");
            e.printStackTrace();
        }
    }
}
