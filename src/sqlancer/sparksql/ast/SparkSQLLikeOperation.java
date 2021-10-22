package sqlancer.sparksql.ast;

import sqlancer.LikeImplementationHelper;
import sqlancer.common.ast.BinaryNode;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;

public class SparkSQLLikeOperation extends BinaryNode<SparkSQLExpression> implements SparkSQLExpression {

    public SparkSQLLikeOperation(SparkSQLExpression left, SparkSQLExpression right) {
        super(left, right);
    }

    @Override
    public SparkSQLDataType getExpressionType() {
        return SparkSQLDataType.BOOLEAN;
    }

    @Override
    public SparkSQLConstant getExpectedValue() {
        SparkSQLConstant leftVal = getLeft().getExpectedValue();
        SparkSQLConstant rightVal = getRight().getExpectedValue();
        if (leftVal == null || rightVal == null) {
            return null;
        }
        if (leftVal.isNull() || rightVal.isNull()) {
            return SparkSQLConstant.createNullConstant();
        } else {
            boolean val = LikeImplementationHelper.match(leftVal.asString(), rightVal.asString(), 0, 0, true);
            return SparkSQLConstant.createBooleanConstant(val);
        }
    }

    @Override
    public String getOperatorRepresentation() {
        return "LIKE";
    }

}