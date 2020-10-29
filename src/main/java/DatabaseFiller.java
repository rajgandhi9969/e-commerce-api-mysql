/***
 * Author: Raj Pradeep Gandhi
 * This program fills the database. This intended to be executed when blank database is provided.
 */

import com.devskiller.jfairy.Fairy;
import com.devskiller.jfairy.producer.text.TextProducer;
import com.mysql.cj.jdbc.MysqlDataSource;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;


class DatabaseFiller {

    public static int getRandomNumber(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }

    /**
     * This method is used to clear the db before it can filled with data
     */
    public void clearDB() {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getMySQLDataSource().getConnection();
            conn.setAutoCommit(false);
            stmt = conn.createStatement();
            stmt.executeQuery("SET FOREIGN_KEY_CHECKS=0 ");
            stmt.executeUpdate("Truncate table reviews");
            stmt.executeUpdate("Truncate table order_details");
            stmt.executeUpdate("Truncate table orders");
            stmt.executeUpdate("Truncate table products");
            stmt.executeUpdate("Truncate table users");
            stmt.executeUpdate("SET FOREIGN_KEY_CHECKS=1 ");
            conn.commit();

        } catch (SQLException sqlException) {
            try {
                stmt.executeQuery("SET FOREIGN_KEY_CHECKS=1 ");
                conn.rollback();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
            sqlException.printStackTrace();
        } finally {
            try {
                stmt.close();
                conn.close();
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
            }
        }
    }

    /***
     * Method for filling random users
     * @param totalEntries: Number of users to be filled
     */
    public void fillUsers(int totalEntries) {

        StoreAPI storeOperations = new StoreAPIMysqlImpl();
        for (int i = 0; i < totalEntries; i++) {
            storeOperations.createAccount("user_" + i, "password_" + i, Fairy.create().person().getFirstName(), Fairy.create().person().getLastName());
        }
    }

    /***
     * This method is used to fill random products to prdducts table
     * @param totalEntries: number of products to be filled
     */
    public void fillProducts(int totalEntries) {
        Fairy fairy = Fairy.create();
        TextProducer text = fairy.textProducer();
        StoreAPI storeOperations = new StoreAPIMysqlImpl();
        for (int i = 0; i < totalEntries; i++) {
            storeOperations.addProduct(i, text.sentence(getRandomNumber(1, 10)), text.paragraph(getRandomNumber(1, 50)), (float) getRandomNumber(1, 3000), getRandomNumber(1, 5000));
        }
    }

    /**
     * Method for random orders to be placed
     *
     * @param totalEntries: Total number of orders to filled
     */
    public void fillOrders(int totalEntries) {
        StoreAPI storeOperations = new StoreAPIMysqlImpl();
        for (int i = 0; i < totalEntries; i++) {
            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            Date date = new Date();
            int getRandomUserId = getRandomNumber(0, 999);
            storeOperations.submitOrder(dateFormat.format(date), "user_" + getRandomUserId, "password_" + getRandomUserId, getProductsToBeOrdered(getRandomNumber(1, 10)));
        }
    }

    /***
     * This method map of products and quantities to be ordered for a particular order
     * @param totalEntries: number of products in a order
     * @return: Map of products and their quantities that are to be required.
     */
    public HashMap<Long, Integer> getProductsToBeOrdered(int totalEntries) {
        HashMap<Long, Integer> listOfProducts = new HashMap<>();
        for (int i = 0; i < totalEntries; i++) {
            listOfProducts.put((long) getRandomNumber(0, 9999), getRandomNumber(1, 10));
        }
        return listOfProducts;
    }

    /***
     * This method is used to fill random reviews in the reviews table
     * @param totalEntries: number reviews to be filled.
     */
    public void fillReviews(int totalEntries) {
        StoreAPI storeOperations = new StoreAPIMysqlImpl();
        for (int i = 0; i < totalEntries; i++) {
            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            Date date = new Date();
            int getRandomUserId = getRandomNumber(0, 999);
            Fairy fairy = Fairy.create();
            TextProducer text = fairy.textProducer();
            storeOperations.postReview(i, "user_" + getRandomUserId, "password_" + getRandomUserId, (long) getRandomNumber(0, 9999), getRandomNumber(1, 5), text.paragraph(getRandomNumber(1, 5)), dateFormat.format(date));
        }
    }

    /***
     * This method helps in setting the MySQL datasource which helps in connecting with the database
     * @return MySQL datasource
     */
    public MysqlDataSource getMySQLDataSource() {
        Properties dbConnectionProp = new Properties();
        MysqlDataSource dataSource = null;
        FileInputStream dbPropFile = null;
        try {
            dbPropFile = new FileInputStream("properties/db.properties");
            dbConnectionProp.load(dbPropFile);
            dbPropFile.close();
            dataSource = new MysqlDataSource();
            dataSource.setURL(dbConnectionProp.getProperty("url"));
            dataSource.setUser(dbConnectionProp.getProperty("username"));
            dataSource.setPassword(dbConnectionProp.getProperty("password"));
        } catch (FileNotFoundException e) {

            System.out.println("Property File containing db details is not not found");
        } catch (IOException io) {
            System.out.println("Error reading the file");
        }
        return dataSource;
    }
}
