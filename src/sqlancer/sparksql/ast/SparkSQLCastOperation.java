package sqlancer.sparksql.ast;

import sqlancer.sparksql.SparkSQLCompoundDataType;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;

public class SparkSQLCastOperation implements SparkSQLExpression {

    private final SparkSQLExpression expression;
    private final SparkSQLDataType type;

    public SparkSQLCastOperation(SparkSQLExpression expression, SparkSQLDataType type) {
        if (expression == null) {
            throw new AssertionError();
        }
        this.expression = expression;
        this.type = type;
    }

    @Override
    public SparkSQLDataType getExpressionType() {
        return type;
    }

    @Override
    public SparkSQLConstant getExpectedValue() {
        SparkSQLConstant expectedValue = expression.getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return expectedValue.cast(type);
    }

    public SparkSQLExpression getExpression() {
        return expression;
    }

}