package sqlancer.sparksql.ast;

import sqlancer.sparksql.SparkSQLCompoundDataType;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;

public class SparkSQLCastOperation implements SparkSQLExpression {

    private final SparkSQLExpression expression;
    private final SparkSQLCompoundDataType type;

    public SparkSQLCastOperation(SparkSQLExpression expression, SparkSQLCompoundDataType type) {
        if (expression == null) {
            throw new AssertionError();
        }
        this.expression = expression;
        this.type = type;
    }

    @Override
    public SparkSQLDataType getExpressionType() {
        return type.getDataType();
    }

    @Override
    public SparkSQLConstant getExpectedValue() {
        SparkSQLConstant expectedValue = expression.getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return expectedValue.cast(type.getDataType());
    }

    public SparkSQLExpression getExpression() {
        return expression;
    }

    public SparkSQLDataType getType() {
        return type.getDataType();
    }

    public SparkSQLCompoundDataType getCompoundType() {
        return type;
    }

}