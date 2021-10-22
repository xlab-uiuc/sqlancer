package sqlancer.sparksql.ast;

import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;

public interface SparkSQLExpression {

    default SparkSQLDataType getExpressionType() {
        return null;
    }

    default SparkSQLConstant getExpectedValue() {
        return null;
    }

}
