package sqlancer.sparksql.ast;

import sqlancer.sparksql.SparkSQLSchema.SparkSQLColumn;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;

public class SparkSQLColumnValue implements SparkSQLExpression {

    private final SparkSQLColumn c;
    private final SparkSQLConstant expectedValue;

    public SparkSQLColumnValue(SparkSQLColumn c, SparkSQLConstant expectedValue) {
        this.c = c;
        this.expectedValue = expectedValue;
    }

    @Override
    public SparkSQLDataType getExpressionType() {
        return c.getType();
    }

    @Override
    public SparkSQLConstant getExpectedValue() {
        return expectedValue;
    }

    public static SparkSQLColumnValue create(SparkSQLColumn c, SparkSQLConstant expected) {
        return new SparkSQLColumnValue(c, expected);
    }

    public SparkSQLColumn getColumn() {
        return c;
    }

}