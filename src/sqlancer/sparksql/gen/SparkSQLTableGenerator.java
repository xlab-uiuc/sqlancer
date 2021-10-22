package sqlancer.sparksql.gen;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.sparksql.SparkSQLSchema;
import sqlancer.sparksql.gen.SparkSQLExpressionGenerator;
import sqlancer.sparksql.gen.SparkSQLTableGenerator;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;
import sqlancer.sparksql.SparkSQLProvider.SparkSQLGlobalState;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLColumn;
import sqlancer.sparksql.ast.SparkSQLExpression;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

// TODO: can reference MySQLTableGenerator for example on how to generate tables
public class SparkSQLTableGenerator {
    private final StringBuilder sb = new StringBuilder();
    private final String tableName;
    private int columnId;
    private final List<String> columns = new ArrayList<>();
    private final SparkSQLSchema schema;
    private final SparkSQLGlobalState globalState;
    private final List<SparkSQLSchema.SparkSQLColumn> columnsToBeAdded = new ArrayList<>();

    public SparkSQLTableGenerator(SparkSQLGlobalState globalState, String tableName) {
        this.tableName = tableName;
        this.schema = globalState.getSchema();
        this.globalState = globalState;
        // this.table
        // TODO: add errors
    }

    public static SQLQueryAdapter generate(SparkSQLGlobalState globalState, String tableName) {
        return new SparkSQLTableGenerator(globalState, tableName).create();
    }

    // TODO: implement
    private SQLQueryAdapter create() {
        ExpectedErrors errors = new ExpectedErrors();

        sb.append("CREATE");
        boolean external = false;
        if (Randomly.getBoolean()) {
            sb.append(" EXTERNAL");
            external = true;
        }
        sb.append(" TABLE");
        if (Randomly.getBoolean()) {
            sb.append(" IF NOT EXISTS");
        }
        sb.append(" ");
        sb.append(tableName);
        if (Randomly.getBoolean() && !schema.getDatabaseTables().isEmpty()) {
            createLike();
        } else {
            createStandard();
        }
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private void createStandard() {
        sb.append("(");
        for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            appendColumn();
        }
        sb.append(") ");
        generatePartitionBy();
        appendTableOptions();
//            if ((tableHasNullableColumn || setPrimaryKey) && engine == MySQLSchema.MySQLTable.MySQLEngine.CSV) {
//                if (true) { // TODO
//                    // results in an error
//                    throw new IgnoreMeException();
//                }
//            } else if ((tableHasNullableColumn || keysSpecified > 1) && engine == MySQLSchema.MySQLTable.MySQLEngine.ARCHIVE) {
//                errors.add("Too many keys specified; max 1 keys allowed");
//                errors.add("Table handler doesn't support NULL in given index");
//                addCommonErrors(errors);
//                return new SQLQueryAdapter(sb.toString(), errors, true);
//            }
//            addCommonErrors(errors);

    }

    private void createLike() {
        sb.append(" LIKE ");
        sb.append(schema.getRandomTable().getName());
    }

    // TODO: implement
    public SQLQueryAdapter getQuery(SparkSQLGlobalState globalState) {
        return null;
    }

    // TODO: implement
    public static String getRandomCollate() {
        return null;
    }

    private void appendColumn() {
        String columnName = DBMSCommon.createColumnName(columnId);
        columns.add(columnName);
        sb.append(columnName);
        // TODO: move nested type generation to DataType
        //  appendTypeString should only unravel DataType
        SparkSQLDataType randomType = SparkSQLDataType.getRandomType();
        appendTypeString(randomType);
        columnId++;
    }

    private enum TableOptions {
        // TBLPROPERTIES, COMMENT seem unnecessary
        // TODO: implement AS when SELECT is done
        // TODO: implement CLUSTERED/SORTED BY if potentially useful
        ROW_FORMAT, STORED_AS, LOCATION;
        // AS;
        public static List<SparkSQLTableGenerator.TableOptions> getRandomTableOptions() {
            List<SparkSQLTableGenerator.TableOptions> options;
            // try to ensure that usually, only a few of these options are generated
            if (Randomly.getBooleanWithSmallProbability()) {
                options = Randomly.subset(SparkSQLTableGenerator.TableOptions.values());
            } else {
                if (Randomly.getBoolean()) {
                    options = Collections.emptyList();
                } else {
                    options = Randomly.nonEmptySubset(Arrays.asList(SparkSQLTableGenerator.TableOptions.values()), Randomly.smallNumber());
                }
            }
            return options;
        }
    }

    private void appendTableOptions() {
        List<SparkSQLTableGenerator.TableOptions> tableOptions = SparkSQLTableGenerator.TableOptions.getRandomTableOptions();
        int i = 0;
        for (SparkSQLTableGenerator.TableOptions o : tableOptions) {
            if (i++ != 0) {
                sb.append(", ");
            }
            switch (o) {
                case ROW_FORMAT:
                    appendRowFormat();
                    break;
                case STORED_AS:
                    appendStoredAs();
                    break;
                default:
                    throw new AssertionError(o);
            }
        }
    }

    private void appendRowFormat() {
        sb.append("ROW FORMAT ");
        String row_format = Randomly.fromOptions("SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'",
                                                 "DELIMITED FIELDS TERMINATED BY ','");
        sb.append(row_format);
    }

    private void appendStoredAs() {
        sb.append("STORED AS ");
        String stored = Randomly.fromOptions("SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'",
                                             "ORC",
                                             "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'",
                                             "TEXTFILE",
                                             "PARQUET");
        sb.append(stored);
    }

    private void generatePartitionBy() {
        if (Randomly.getBoolean()) {
            return;
        }
        sb.append(" PARTITION BY (");
        int n = Randomly.getBooleanWithSmallProbability()? Randomly.smallNumber() + 1 : 1;
        for (int i = 0; i < n; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append("(");
            // column does not need to be in the CREATE TABLE
            // TODO: fix when generating expressions
            SparkSQLExpression expr = SparkSQLExpressionGenerator.generateExpression(globalState, columnsToBeAdded);
            sb.append(SparkSQLVisitor.asString(expr)); // x INT
            sb.append(")");
        }
        sb.append(")");
    }

    private void appendTypeString(SparkSQLDataType randomType, int maxDepth) {
        switch (randomType) {
            case DECIMAL:
                sb.append("DECIMAL");
                optionallyAddPrecisionAndScale(sb);
                break;
            case INT:
                sb.append(Randomly.fromOptions("TINYINT", "SMALLINT", "INT", "BIGINT"));
                if (Randomly.getBoolean()) {
                    sb.append("(");
                    sb.append(Randomly.getNotCachedInteger(0, 255)); // Display width out of range for column 'c0' (max =
                    // 255)
                    sb.append(")");
                }
                break;
            case STRING:
                sb.append(Randomly.fromOptions("STRING"));
                break;
            case FLOAT:
                sb.append("FLOAT");
                optionallyAddPrecisionAndScale(sb);
                break;
            case DOUBLE:
                sb.append(Randomly.fromOptions("DOUBLE", "FLOAT"));
                optionallyAddPrecisionAndScale(sb);
                break;
            case DATETIME:
                sb.append(Randomly.fromOptions("DATE", "TIMESTAMP"));
                break;
            case ARRAY:
                if (maxDepth == 0) {
                    appendTypeString(SparkSQLDataType.getRandomType(), maxDepth);
                    break;
                }
                sb.append("ARRAY<");
                appendTypeString(SparkSQLDataType.getRandomType(), maxDepth-1);
                sb.append(">");
                break;
            case MAP:
                if (maxDepth == 0) {
                    appendTypeString(SparkSQLDataType.getRandomType(), maxDepth);
                    break;
                }
                sb.append("MAP<");
                appendTypeString(SparkSQLDataType.getRandomType(), maxDepth-1);
                sb.append(", ");
                appendTypeString(SparkSQLDataType.getRandomType(), maxDepth-1);
                sb.append(">");
                break;
            case STRUCT:
                if (maxDepth == 0) {
                    appendTypeString(SparkSQLDataType.getRandomType(), maxDepth);
                    break;
                }
                sb.append("STRUCT<");
                // TODO: complete when generating expressions
                appendTypeString(SparkSQLDataType.getRandomType(), maxDepth-1);
                sb.append(">");
                break;
            case BINARY:
                sb.append("BINARY");
                break;
            case BOOLEAN:
                sb.append("BOOLEAN");
                break;
            default:
                throw new AssertionError();
        }
    }

    public static void optionallyAddPrecisionAndScale(StringBuilder sb) {
        if (Randomly.getBoolean()) {
            sb.append("(");
            // The maximum number of digits (M) for DECIMAL is 65
            long m = Randomly.getNotCachedInteger(1, 65);
            sb.append(m);
            sb.append(", ");
            // The maximum number of supported decimals (D) is 30
            long nCandidate = Randomly.getNotCachedInteger(1, 30);
            // For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column 'c0').
            long n = Math.min(nCandidate, m);
            sb.append(n);
            sb.append(")");
        }
    }
}