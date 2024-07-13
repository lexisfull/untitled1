package org.example;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * С помощью JDBC, выполнить следующие пункты:
 * 1. Создать таблицу Person (скопировать код с семниара)
 * 2. Создать таблицу Department (id bigint primary key, name varchar(128) not null)
 * 3. Добавить в таблицу Person поле department_id типа bigint (внешний ключ)
 * 4. Написать метод, который загружает Имя department по Идентификатору person
 **/
public class App {
    public static void main(String[] args) {
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test")) {
            createTable(connection);
            createTable2(connection);
            insertData(connection);
            insertDataIntoDepartment(connection);
            getDepartmentNameByPersonId();
            String age = "55";
            System.out.println("Person с возрастом 55: " + selectNamesByAge(connection, age));

            updateData(connection);
            selectData(connection);
        } catch (SQLException e) {
            System.err.println("Во время подключения произошла ошибка: " + e.getMessage());
        }
        ;
    }

    private static void createTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("""
                create table person (
                  id bigint primary key,
                  name varchar(256),
                  age integer,
                  active boolean,
                  department_id bigint
                )
                """);
        } catch (SQLException e) {
            System.err.println("Во время создания таблицы произошла ошибка: " + e.getMessage());
            throw e;
        }
    }

    private static void insertData(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            StringBuilder insertQuery = new StringBuilder("insert into person(id, name, age, active, department_id) values\n");
            for (int i = 1; i <= 10; i++) {
                int age = ThreadLocalRandom.current().nextInt(20, 60);
                boolean active = ThreadLocalRandom.current().nextBoolean();
                int departmentId = ThreadLocalRandom.current().nextInt(1, 6);
                insertQuery.append(String.format("(%s, '%s', %s, %s, %s)", i, "Person #" + i, age, active, departmentId));
                if (i != 10) {
                    insertQuery.append(",\n");
                }
            }
            int insertCount = statement.executeUpdate(insertQuery.toString());
            System.out.println("Вставлено строк в person: " + insertCount);
        }
    }

    private static void updateData(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            int updateCount = statement.executeUpdate("update person set active = true where id > 5");
            System.out.println("Обновлено строк: " + updateCount);
        }
    }


    private static List<String> selectNamesByAge(Connection connection, String age) throws SQLException {

        try (PreparedStatement statement =
                     connection.prepareStatement("select name from person where age = ?")) {
            statement.setInt(1, Integer.parseInt(age));
            ResultSet resultSet = statement.executeQuery();

            List<String> names = new ArrayList<>();
            while (resultSet.next()) {
                names.add(resultSet.getString("name"));
            }
            return names;
        }
    }

    private static void selectData(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("""
                    select id, name, age
                    from person
                    where active is true""");

            while (resultSet.next()) {
                long id = resultSet.getLong("id");
                String name = resultSet.getString("name");
                int age = resultSet.getInt("age");
                System.out.println("Найдена строка: [id = " + id + ", name = " + name + ", age = " + age + "]");
            }
        }
    }
    private static void createTable2(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("""
                    create table Department (
                      id bigint primary key,
                      name varchar(128) not null
                     )
                    """);
        } catch (SQLException e) {
            System.err.println("Во время создания таблицы произошла ошибка: " + e.getMessage());
            throw e;
        }
    }
    private static void insertDataIntoDepartment(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            StringBuilder insertQuery = new StringBuilder("insert into Department(id, name) values\n");
            for (int i = 1; i <= 5; i++) {
                insertQuery.append(String.format("(%s, 'Department %s')", i, i));
                if (i != 5) {
                    insertQuery.append(",\n");
                }
            }
            int insertCount = statement.executeUpdate(insertQuery.toString());
            System.out.println("Вставлено строк в Department: " + insertCount);
        }
    }
    private static String getDepartmentNameByPersonId(Connection connection, long personId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT department.name\n" +
                        "FROM person person\n" +
                        "JOIN Department d ON p.department_id = d.id\n" +
                        "WHERE p.id = ?")) {
            statement.setLong(1, personId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getString("name");
                } else {
                    return "Не установлен!";
                }
            }
        }
    }

}