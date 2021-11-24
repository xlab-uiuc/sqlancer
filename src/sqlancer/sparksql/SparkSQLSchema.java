package sqlancer.sparksql;

import org.postgresql.util.PSQLException;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.DBMSCommon;
import sqlancer.common.schema.*;
import sqlancer.sparksql.ast.SparkSQLConstant;
import sqlancer.sparksql.SparkSQLProvider.SparkSQLGlobalState;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLTable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class SparkSQLSchema extends AbstractSchema<SparkSQLGlobalState, SparkSQLTable> {

    // TODO: implement random generation of nested types
    public enum SparkSQLDataType {
        // TODO: group similar types
        // NAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DECIMAL, STRING, VARCHAR, CHAR, BINARY, BOOLEAN, TIMESTAMP, DATE;
        BOOLEAN, INT, DECIMAL, FLOAT, DOUBLE, STRING;
        //ARRAY, MAP, STRUCT, STRUCT_FIELD;
        public static SparkSQLDataType getRandomType() {
            return Randomly.fromOptions(values());
        }
    }

//    public static class SparkSQLCompositeDataType {
//
//        private final SparkSQLDataType dataType;
//
//        private final int size;
//
//        public SparkSQLCompositeDataType(SparkSQLDataType dataType, int size) {
//            this.dataType = dataType;
//            this.size = size;
//        }
//
//        public SparkSQLDataType getPrimitiveDataType() {
//            return dataType;
//        }
//
//        public int getSize() {
//            if (size == -1) {
//                throw new AssertionError(this);
//            }
//            return size;
//        }
//
//        public static SparkSQLCompositeDataType getRandom() {
//            SparkSQLDataType type = SparkSQLDataType.getRandom();
//            return null;
//        }
//
//        @Override
//        // TODO: implement
//        public String toString() {
//            switch (getPrimitiveDataType()) {
//
//            default:
//                throw new AssertionError(getPrimitiveDataType());
//            }
//        }
//
//    }

    // TODO: implement
    public static class SparkSQLColumn extends AbstractTableColumn<SparkSQLTable, SparkSQLDataType> {

        public SparkSQLColumn(String name, SparkSQLDataType columnType) {
            super(name, null, columnType);
        }

    }

    public static SparkSQLDataType getColumnType(String typeString) {
        switch (typeString) {
            case "int":
                return SparkSQLDataType.INT;
            case "string":
                return SparkSQLDataType.STRING;
            // TODO: get the other strings
            default:
                throw new AssertionError(typeString);
        }
    }

    public static class SparkSQLRowValue extends AbstractRowValue<SparkSQLTables, SparkSQLColumn, SparkSQLConstant> {

        protected SparkSQLRowValue(SparkSQLTables tables, Map<SparkSQLColumn, SparkSQLConstant> values) {
            super(tables, values);
        }

    }

    // TODO: implement
    public static class SparkSQLTables extends AbstractTables<SparkSQLTable, SparkSQLColumn> {

        public SparkSQLTables(List<SparkSQLTable> tables) {
            super(tables);
        }

        public SparkSQLRowValue getRandomRowValue(SQLConnection con) throws SQLException {
            String randomRow = String.format("SELECT %s FROM %s ORDER BY RANDOM() LIMIT 1", columnNamesAsString(
                            c -> c.getTable().getName() + "." + c.getName() + " AS " + c.getTable().getName() + c.getName()),
                    // columnNamesAsString(c -> "typeof(" + c.getTable().getName() + "." +
                    // c.getName() + ")")
                    tableNamesAsString());
            Map<SparkSQLColumn, SparkSQLConstant> values = new HashMap<>();
            try (Statement s = con.createStatement()) {
                ResultSet randomRowValues = s.executeQuery(randomRow);
                if (!randomRowValues.next()) {
                    throw new AssertionError("could not find random row! " + randomRow + "\n");
                }
                for (int i = 0; i < getColumns().size(); i++) {
                    SparkSQLColumn column = getColumns().get(i);
                    int columnIndex = randomRowValues.findColumn(column.getTable().getName() + column.getName());
                    assert columnIndex == i + 1;
                    SparkSQLConstant constant;
                    if (randomRowValues.getString(columnIndex) == null) {
                        constant = SparkSQLConstant.createNullConstant();
                    } else {
                        switch (column.getType()) {
                            case INT:
                                constant = SparkSQLConstant.createIntConstant(randomRowValues.getLong(columnIndex));
                                break;
                            case BOOLEAN:
                                constant = SparkSQLConstant.createBooleanConstant(randomRowValues.getBoolean(columnIndex));
                                break;
                            case STRING:
                                constant = SparkSQLConstant.createStringConstant(randomRowValues.getString(columnIndex));
                                break;
                            default:
                                throw new IgnoreMeException();
                        }
                    }
                    values.put(column, constant);
                }
                assert !randomRowValues.next();
                return new SparkSQLSchema.SparkSQLRowValue(this, values);
            } catch (PSQLException e) {
                // TODO: find SparkSQL exception
                throw new IgnoreMeException();
            }

        }
    }

    // TODO: implement
    public SparkSQLSchema(List<SparkSQLTable> databaseTables) {
        super(databaseTables);
    }

    public SparkSQLTables getRandomTableNonEmptyTables() {
        return new SparkSQLTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    public static class SparkSQLTable extends AbstractRelationalTable<SparkSQLColumn, TableIndex, SparkSQLGlobalState> {

        public enum TableType {
            STANDARD, EXTERNAL
        }

        private final TableType tableType;

        public SparkSQLTable(String tableName, List<SparkSQLColumn> columns,
                             TableType tableType) {
            super(tableName, columns, null, false);
            this.tableType = tableType;
        }

        public TableType getTableType() {
            return tableType;
        }

    }

    // TODO: implement
    public static SparkSQLSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<SparkSQLTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con);
        for (String tableName : tableNames) {
            if (DBMSCommon.matchesIndexName(tableName)) {
                continue; // TODO: unexpected?
            }
            List<SparkSQLColumn> databaseColumns = getTableColumns(con, tableName);
            SparkSQLTable t = new SparkSQLTable(tableName, databaseColumns, null);
            for (SparkSQLColumn c : databaseColumns) {
                c.setTable(t);
            }
            databaseTables.add(t);

        }
        return new SparkSQLSchema(databaseTables);
    }

    // TODO: implement
    private static List<String> getTableNames(SQLConnection con) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            ResultSet tableRs = s.executeQuery("SHOW TABLES");
            while (tableRs.next()) {
                String tableName = tableRs.getString(2);
                tableNames.add(tableName);
            }
        }
        return tableNames;
    }

    // TODO: implement
    private static List<SparkSQLColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<SparkSQLColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s
                    .executeQuery("DESCRIBE TABLE " + tableName)) {
                while (rs.next()) {
                    String columnName = rs.getString("col_name");
                    String dataType = rs.getString("data_type");
                    SparkSQLColumn c = new SparkSQLColumn(columnName, getColumnType(dataType));
                    // TODO: do we need to remove #?
                    columns.add(c);
                }
            }
        }
        return columns;
    }

}
