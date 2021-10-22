package sqlancer.sparksql.ast;

import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;
import sqlancer.sparksql.ast.SparkSQLBinaryComparisonOperation.SparkSQLBinaryComparisonOperator;
import sqlancer.sparksql.ast.SparkSQLBinaryLogicalOperation.BinaryLogicalOperator;

public final class SparkSQLBetweenOperation implements SparkSQLExpression {

    private final SparkSQLExpression expr;
    private final SparkSQLExpression left;
    private final SparkSQLExpression right;
    private final boolean isSymmetric;

    public SparkSQLBetweenOperation(SparkSQLExpression expr, SparkSQLExpression left, SparkSQLExpression right,
            boolean symmetric) {
        this.expr = expr;
        this.left = left;
        this.right = right;
        isSymmetric = symmetric;
    }

    public SparkSQLExpression getExpr() {
        return expr;
    }

    public SparkSQLExpression getLeft() {
        return left;
    }

    public SparkSQLExpression getRight() {
        return right;
    }

    public boolean isSymmetric() {
        return isSymmetric;
    }

    @Override
    public SparkSQLConstant getExpectedValue() {
        SparkSQLBinaryComparisonOperation leftComparison = new SparkSQLBinaryComparisonOperation(left, expr,
                SparkSQLBinaryComparisonOperator.LESS_EQUALS);
        SparkSQLBinaryComparisonOperation rightComparison = new SparkSQLBinaryComparisonOperation(expr, right,
                SparkSQLBinaryComparisonOperator.LESS_EQUALS);
        SparkSQLBinaryLogicalOperation andOperation = new SparkSQLBinaryLogicalOperation(leftComparison,
                rightComparison, SparkSQLBinaryLogicalOperation.BinaryLogicalOperator.AND);
        if (isSymmetric) {
            SparkSQLBinaryComparisonOperation leftComparison2 = new SparkSQLBinaryComparisonOperation(right, expr,
                    SparkSQLBinaryComparisonOperator.LESS_EQUALS);
            SparkSQLBinaryComparisonOperation rightComparison2 = new SparkSQLBinaryComparisonOperation(expr, left,
                    SparkSQLBinaryComparisonOperator.LESS_EQUALS);
            SparkSQLBinaryLogicalOperation andOperation2 = new SparkSQLBinaryLogicalOperation(leftComparison2,
                    rightComparison2, SparkSQLBinaryLogicalOperation.BinaryLogicalOperator.AND);
            SparkSQLBinaryLogicalOperation orOp = new SparkSQLBinaryLogicalOperation(andOperation, andOperation2,
                    BinaryLogicalOperator.OR);
            return orOp.getExpectedValue();
        } else {
            return andOperation.getExpectedValue();
        }
    }

    @Override
    public SparkSQLDataType getExpressionType() {
        return SparkSQLDataType.BOOLEAN;
    }

}