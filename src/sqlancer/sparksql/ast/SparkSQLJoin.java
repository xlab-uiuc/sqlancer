package sqlancer.sparksql.ast;

import sqlancer.Randomly;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;

public class SparkSQLJoin implements SparkSQLExpression {

    public enum SparkSQLJoinType {
        INNER, LEFT, RIGHT, FULL, CROSS, SEMI, ANTI;

        public static SparkSQLJoinType getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    private final SparkSQLExpression tableReference;
    private final SparkSQLExpression onClause;
    private final SparkSQLJoinType type;

    public SparkSQLJoin(SparkSQLExpression tableReference, SparkSQLExpression onClause, SparkSQLJoinType type) {
        this.tableReference = tableReference;
        this.onClause = onClause;
        this.type = type;
    }

    public SparkSQLExpression getTableReference() {
        return tableReference;
    }

    public SparkSQLExpression getOnClause() {
        return onClause;
    }

    public SparkSQLJoinType getType() {
        return type;
    }

    @Override
    public SparkSQLDataType getExpressionType() {
        throw new AssertionError();
    }

    @Override
    public SparkSQLConstant getExpectedValue() {
        throw new AssertionError();
    }

}