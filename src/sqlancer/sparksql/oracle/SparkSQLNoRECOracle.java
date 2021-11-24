package sqlancer.sparksql.oracle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.oracle.NoRECBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.sparksql.SparkSQLCompoundDataType;
import sqlancer.sparksql.SparkSQLSchema;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLColumn;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLTable;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLTables;
import sqlancer.sparksql.SparkSQLVisitor;
import sqlancer.sparksql.SparkSQLProvider.SparkSQLGlobalState;
import sqlancer.sparksql.ast.SparkSQLCastOperation;
import sqlancer.sparksql.ast.SparkSQLColumnValue;
import sqlancer.sparksql.ast.SparkSQLExpression;
import sqlancer.sparksql.ast.SparkSQLSelect;
import sqlancer.sparksql.ast.SparkSQLSelect.SparkSQLFromTable;
import sqlancer.sparksql.ast.SparkSQLSelect.SelectType;
import sqlancer.sparksql.gen.SparkSQLExpressionGenerator;

public class SparkSQLNoRECOracle extends NoRECBase<SparkSQLGlobalState> implements TestOracle {

    private final SparkSQLSchema s;

    public SparkSQLNoRECOracle(SparkSQLGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        // SparkSQLCommon.addCommonExpressionErrors(errors);
        // SparkSQLCommon.addCommonFetchErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        SparkSQLTables randomTables = s.getRandomTableNonEmptyTables();
        List<SparkSQLColumn> columns = randomTables.getColumns();
        SparkSQLExpression randomWhereCondition = getRandomWhereCondition(columns);
        List<SparkSQLTable> tables = randomTables.getTables();

        //List<SparkSQLJoin> joinStatements = getJoinStatements(state, columns, tables);
        List<SparkSQLExpression> fromTables = tables.stream().map(t -> new SparkSQLFromTable(t, Randomly.getBoolean()))
                .collect(Collectors.toList());
        int secondCount = getUnoptimizedQueryCount(fromTables, randomWhereCondition);
        int firstCount = getOptimizedQueryCount(fromTables, columns, randomWhereCondition);
        if (firstCount == -1 || secondCount == -1) {
            throw new IgnoreMeException();
        }
        if (firstCount != secondCount) {
            String queryFormatString = "-- %s;\n-- count: %d";
            String firstQueryStringWithCount = String.format(queryFormatString, optimizedQueryString, firstCount);
            String secondQueryStringWithCount = String.format(queryFormatString, unoptimizedQueryString, secondCount);
            state.getState().getLocalState()
                    .log(String.format("%s\n%s", firstQueryStringWithCount, secondQueryStringWithCount));
            String assertionMessage = String.format("the counts mismatch (%d and %d)!\n%s\n%s", firstCount, secondCount,
                    firstQueryStringWithCount, secondQueryStringWithCount);
            throw new AssertionError(assertionMessage);
        }
    }

//    public static List<SparkSQLJoin> getJoinStatements(SparkSQLGlobalState globalState, List<SparkSQLColumn> columns,
//                                                       List<SparkSQLTable> tables) {
//        List<SparkSQLJoin> joinStatements = new ArrayList<>();
//        SparkSQLExpressionGenerator gen = new SparkSQLExpressionGenerator(globalState).setColumns(columns);
//        for (int i = 1; i < tables.size(); i++) {
//            SparkSQLExpression joinClause = gen.generateExpression(SparkSQLDataType.BOOLEAN);
//            SparkSQLTable table = Randomly.fromList(tables);
//            tables.remove(table);
//            SparkSQLJoinType options = SparkSQLJoinType.getRandom();
//            SparkSQLJoin j = new SparkSQLJoin(new SparkSQLFromTable(table, Randomly.getBoolean()), joinClause, options);
//            joinStatements.add(j);
//        }
//        // JOIN subqueries
//        for (int i = 0; i < Randomly.smallNumber(); i++) {
//            SparkSQLTables subqueryTables = globalState.getSchema().getRandomTableNonEmptyTables();
//            SparkSQLSubquery subquery = SparkSQLTLPBase.createSubquery(globalState, String.format("sub%d", i),
//                    subqueryTables);
//            SparkSQLExpression joinClause = gen.generateExpression(SparkSQLDataType.BOOLEAN);
//            SparkSQLJoinType options = SparkSQLJoinType.getRandom();
//            SparkSQLJoin j = new SparkSQLJoin(subquery, joinClause, options);
//            joinStatements.add(j);
//        }
//        return joinStatements;
//    }

    private SparkSQLExpression getRandomWhereCondition(List<SparkSQLColumn> columns) {
        return new SparkSQLExpressionGenerator(state).setColumns(columns).generateExpression(SparkSQLDataType.BOOLEAN);
    }

    private int getUnoptimizedQueryCount(List<SparkSQLExpression> fromTables, SparkSQLExpression randomWhereCondition)
            throws SQLException {
        SparkSQLSelect select = new SparkSQLSelect();
        SparkSQLCastOperation isTrue = new SparkSQLCastOperation(randomWhereCondition, SparkSQLCompoundDataType.create(SparkSQLDataType.INT));
        //SparkSQLPostfixText asText = new SparkSQLPostfixText(isTrue, " as count", null, SparkSQLDataType.INT);
        select.setFetchColumns(Collections.singletonList(isTrue));
        select.setFromList(fromTables);
        select.setSelectType(SelectType.ALL);
        // select.setJoinClauses(joinStatements);
        int secondCount = 0;
        unoptimizedQueryString = "SELECT count(*) FROM (" + SparkSQLVisitor.asString(select) + ")";
        if (options.logEachSelect()) {
            logger.writeCurrent(unoptimizedQueryString);
        }
        SQLQueryAdapter q = new SQLQueryAdapter(unoptimizedQueryString, errors);
        SQLancerResultSet rs;
        try {
            rs = q.executeAndGet(state);
        } catch (Exception e) {
            throw new AssertionError(unoptimizedQueryString, e);
        }
        if (rs == null) {
            return -1;
        }
        if (rs.next()) {
            secondCount += rs.getLong(1);
        }
        rs.close();
        return secondCount;
    }

    private int getOptimizedQueryCount(List<SparkSQLExpression> randomTables, List<SparkSQLColumn> columns,
                                       SparkSQLExpression randomWhereCondition) throws SQLException {
        SparkSQLSelect select = new SparkSQLSelect();
        SparkSQLColumnValue allColumns = new SparkSQLColumnValue(Randomly.fromList(columns), null);
        select.setFetchColumns(Arrays.asList(allColumns));
        select.setFromList(randomTables);
        select.setWhereClause(randomWhereCondition);
        select.setSelectType(SelectType.ALL);
        //select.setJoinClauses(joinStatements);
        int firstCount = 0;
        try (Statement stat = con.createStatement()) {
            optimizedQueryString = SparkSQLVisitor.asString(select);
            if (options.logEachSelect()) {
                logger.writeCurrent(optimizedQueryString);
            }
            try (ResultSet rs = stat.executeQuery(optimizedQueryString)) {
                while (rs.next()) {
                    firstCount++;
                }
            }
        } catch (SQLException e) {
            throw new IgnoreMeException();
        }
        return firstCount;
    }

}
