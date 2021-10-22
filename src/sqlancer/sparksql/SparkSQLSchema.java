package sqlancer.sparksql;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.DBMSCommon;
import sqlancer.common.schema.*;
import sqlancer.sparksql.SparkSQLProvider.SparkSQLGlobalState;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLTable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SparkSQLSchema extends AbstractSchema<SparkSQLGlobalState, SparkSQLTable> {

    // TODO: implement
    public enum SparkSQLDataType {
        NAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DECIMAL, STRING, VARCHAR, CHAR, BINARY, BOOLEAN, TIMESTAMP, DATE, ARRAY, MAP, STRUCT, STRUCT_FIELD;
        public static SparkSQLDataType getRandomType() {
            return Randomly.fromOptions(values());
        }
    }

    // TODO(lilia): unsure if this is a relevant class
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

        private final boolean isPrimaryKey;
        private final boolean isNullable;

        public SparkSQLColumn(String name, SparkSQLDataType columnType, boolean isPrimaryKey, boolean isNullable) {
            super(name, null, columnType);
            this.isPrimaryKey = isPrimaryKey;
            this.isNullable = isNullable;
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

        public boolean isNullable() {
            return isNullable;
        }

    }

    // TODO: implement
    public static class SparkSQLTables extends AbstractTables<SparkSQLTable, SparkSQLColumn> {

        public SparkSQLTables(List<SparkSQLTable> tables) {
            super(tables);
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

        public SparkSQLTable(String tableName, List<SparkSQLColumn> columns, boolean isView) {
            super(tableName, columns, Collections.emptyList(), isView);
        }

    }

    // TODO: implement
    public static SparkSQLSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        return null;
    }

    // TODO: implement
    private static List<String> getTableNames(SQLConnection con) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        return tableNames;
    }

    // TODO: implement
    private static List<SparkSQLColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<SparkSQLColumn> columns = new ArrayList<>();
        return columns;
    }

}
