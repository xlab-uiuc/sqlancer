package sqlancer.sparksql.ast;

import sqlancer.common.ast.BinaryNode;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;

public class SparkSQLConcatOperation extends BinaryNode<SparkSQLExpression> implements SparkSQLExpression {

    public SparkSQLConcatOperation(SparkSQLExpression left, SparkSQLExpression right) {
        super(left, right);
    }

    @Override
    public SparkSQLDataType getExpressionType() {
        return SparkSQLDataType.STRING;
    }

    @Override
    public SparkSQLConstant getExpectedValue() {
        SparkSQLConstant leftExpectedValue = getLeft().getExpectedValue();
        SparkSQLConstant rightExpectedValue = getRight().getExpectedValue();
        if (leftExpectedValue == null || rightExpectedValue == null) {
            return null;
        }
        if (leftExpectedValue.isNull() || rightExpectedValue.isNull()) {
            return SparkSQLConstant.createNullConstant();
        }
        String leftStr = leftExpectedValue.cast(SparkSQLDataType.STRING).getUnquotedTextRepresentation();
        String rightStr = rightExpectedValue.cast(SparkSQLDataType.STRING).getUnquotedTextRepresentation();
        return SparkSQLConstant.createStringConstant(leftStr + rightStr);
    }

    @Override
    public String getOperatorRepresentation() {
        return "||";
    }

}