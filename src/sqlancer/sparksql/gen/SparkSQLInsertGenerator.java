package sqlancer.sparksql.gen;

import com.sun.org.apache.xpath.internal.operations.String;
import sqlancer.Randomly;
import sqlancer.common.gen.AbstractInsertGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mysql.MySQLVisitor;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.gen.PostgresExpressionGenerator;
import sqlancer.sparksql.SparkSQLProvider.SparkSQLGlobalState;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLColumn;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLTable;
import sqlancer.sparksql.ast.SparkSQLConstant;
import sqlancer.sparksql.ast.SparkSQLExpression;
import sqlancer.sparksql.gen.SparkSQLExpressionGenerator;

import java.util.List;
import java.util.stream.Collectors;

public class SparkSQLInsertGenerator {

    public SparkSQLInsertGenerator() {
    }

    public static SQLQueryAdapter insert(SparkSQLGlobalState globalState) {
        SparkSQLTable table = globalState.getSchema().getRandomTable();
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        List<SparkSQLColumn> columns = table.getRandomNonEmptyColumnSubset();
        sb.append("INSERT INTO ");
        sb.append(table.getName());
        if (Randomly.getBooleanWithRatherLowProbability()) {
            sb.append("TABLE");
            sb.append(globalState.getSchema().getRandomTable().getName());
            return new SQLQueryAdapter(sb.toString(), errors);
        }
        // TODO: FROM SELECT pattern
        // TODO: implement PARTITION
        // if (Randomly.getBooleanWithRatherLowProbability()) {
        //     sb.append("PARTITION (");
        //     sb.append(")");
        // }
        sb.append(" VALUES ");
        int nrRows;
        if (Randomly.getBoolean()) {
            nrRows = 1;
        } else {
            nrRows = 1 + Randomly.smallNumber();
        }
        for (int row = 0; row < nrRows; row++) {
            if (row != 0) {
                sb.append(", ");
            }
            insertRow(globalState, sb, columns);
        }
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private static void insertRow(SparkSQLGlobalState globalState, StringBuilder sb, List<SparkSQLColumn> columns) {
        sb.append("(");
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            SparkSQLExpression generateConstant;
            if (Randomly.getBoolean()) {
                generateConstant = SparkSQLExpressionGenerator.generateConstant(globalState.getRandomly(),
                        columns.get(i).getType());
            } else {
                generateConstant = new SparkSQLExpressionGenerator(globalState)
                        .generateExpression(columns.get(i).getType());
            }
            sb.append(SparkSQLVisitor.asString(generateConstant));
        }
        sb.append(")");
    }


}
