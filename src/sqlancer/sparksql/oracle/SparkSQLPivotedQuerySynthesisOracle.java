package sqlancer.sparksql.oracle;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.oracle.PivotedQuerySynthesisBase;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.sparksql.SparkSQLProvider.SparkSQLGlobalState;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLColumn;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLRowValue;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLTables;
import sqlancer.sparksql.SparkSQLVisitor;
import sqlancer.sparksql.ast.SparkSQLColumnValue;
import sqlancer.sparksql.ast.SparkSQLConstant;
import sqlancer.sparksql.ast.SparkSQLExpression;
import sqlancer.sparksql.ast.SparkSQLPostfixOperation;
import sqlancer.sparksql.ast.SparkSQLPostfixOperation.PostfixOperator;
import sqlancer.sparksql.ast.SparkSQLSelect;
import sqlancer.sparksql.ast.SparkSQLSelect.SparkSQLFromTable;
import sqlancer.sparksql.gen.SparkSQLExpressionGenerator;

public class SparkSQLPivotedQuerySynthesisOracle
        extends PivotedQuerySynthesisBase<SparkSQLGlobalState, SparkSQLRowValue, SparkSQLExpression, SQLConnection> {

    private List<SparkSQLColumn> fetchColumns;

    public SparkSQLPivotedQuerySynthesisOracle(SparkSQLGlobalState globalState) throws SQLException {
        super(globalState);
    }

    @Override
    public SQLQueryAdapter getRectifiedQuery() throws SQLException {
        SparkSQLTables randomFromTables = globalState.getSchema().getRandomTableNonEmptyTables();

        SparkSQLSelect selectStatement = new SparkSQLSelect();
        selectStatement.setSelectType(Randomly.fromOptions(SparkSQLSelect.SelectType.values()));
        List<SparkSQLColumn> columns = randomFromTables.getColumns();
        pivotRow = randomFromTables.getRandomRowValue(globalState.getConnection());

        fetchColumns = columns;
        selectStatement.setFromList(randomFromTables.getTables().stream().map(t -> new SparkSQLFromTable(t))
                .collect(Collectors.toList()));
        selectStatement.setFetchColumns(fetchColumns.stream()
                .map(c -> new SparkSQLColumnValue(getFetchValueAliasedColumn(c), pivotRow.getValues().get(c)))
                .collect(Collectors.toList()));
        SparkSQLExpression whereClause = generateRectifiedExpression(columns, pivotRow);
        selectStatement.setWhereClause(whereClause);
        List<SparkSQLExpression> groupByClause = generateGroupByClause(columns, pivotRow);
        selectStatement.setGroupByExpressions(groupByClause);
        SparkSQLExpression limitClause = generateLimit();
        selectStatement.setLimitClause(limitClause);
        if (limitClause != null) {
            SparkSQLExpression offsetClause = generateOffset();
            selectStatement.setOffsetClause(offsetClause);
        }
        List<SparkSQLExpression> orderBy = new SparkSQLExpressionGenerator(globalState).setColumns(columns)
                .generateOrderBy();
        selectStatement.setOrderByExpressions(orderBy);
        return new SQLQueryAdapter(SparkSQLVisitor.asString(selectStatement));
    }

    /*
     * Prevent name collisions by aliasing the column.
     */
    private SparkSQLColumn getFetchValueAliasedColumn(SparkSQLColumn c) {
        SparkSQLColumn aliasedColumn = new SparkSQLColumn(c.getName() + " AS " + c.getTable().getName() + c.getName(),
                c.getType());
        aliasedColumn.setTable(c.getTable());
        return aliasedColumn;
    }

    private List<SparkSQLExpression> generateGroupByClause(List<SparkSQLColumn> columns, SparkSQLRowValue rw) {
        if (Randomly.getBoolean()) {
            return columns.stream().map(c -> SparkSQLColumnValue.create(c, rw.getValues().get(c)))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private SparkSQLConstant generateLimit() {
        if (Randomly.getBoolean()) {
            return SparkSQLConstant.createIntConstant(Integer.MAX_VALUE);
        } else {
            return null;
        }
    }

    private SparkSQLExpression generateOffset() {
        if (Randomly.getBoolean()) {
            return SparkSQLConstant.createIntConstant(0);
        } else {
            return null;
        }
    }

    private SparkSQLExpression generateRectifiedExpression(List<SparkSQLColumn> columns, SparkSQLRowValue rw) {
        SparkSQLExpression expr = new SparkSQLExpressionGenerator(globalState).setColumns(columns).setRowValue(rw)
                .generateExpressionWithExpectedResult(SparkSQLDataType.BOOLEAN);
        SparkSQLExpression result;
        if (expr.getExpectedValue().isNull()) {
            result = SparkSQLPostfixOperation.create(expr, PostfixOperator.IS_NULL);
        } else {
            result = SparkSQLPostfixOperation.create(expr,
                    expr.getExpectedValue().cast(SparkSQLDataType.BOOLEAN).asBoolean() ? PostfixOperator.IS_TRUE
                            : PostfixOperator.IS_FALSE);
        }
        rectifiedPredicates.add(result);
        return result;
    }

    @Override
    protected Query<SQLConnection> getContainmentCheckQuery(Query<?> query) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM ("); // ANOTHER SELECT TO USE ORDER BY without restrictions
        sb.append(query.getUnterminatedQueryString());
        sb.append(") as result WHERE ");
        int i = 0;
        for (SparkSQLColumn c : fetchColumns) {
            if (i++ != 0) {
                sb.append(" AND ");
            }
            sb.append("result.");
            sb.append(c.getTable().getName());
            sb.append(c.getName());
            if (pivotRow.getValues().get(c).isNull()) {
                sb.append(" IS NULL");
            } else {
                sb.append(" = ");
                sb.append(pivotRow.getValues().get(c).getTextRepresentation());
            }
        }
        String resultingQueryString = sb.toString();
        return new SQLQueryAdapter(resultingQueryString, errors);
    }

    @Override
    protected String getExpectedValues(SparkSQLExpression expr) {
        return SparkSQLVisitor.asExpectedValues(expr);
    }

}
