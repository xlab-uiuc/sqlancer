package sqlancer.sparksql.ast;

import java.util.List;

import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;

public class SparkSQLInOperation implements SparkSQLExpression {

    private final SparkSQLExpression expr;
    private final List<SparkSQLExpression> listElements;
    private final boolean isTrue;

    public SparkSQLInOperation(SparkSQLExpression expr, List<SparkSQLExpression> listElements, boolean isTrue) {
        this.expr = expr;
        this.listElements = listElements;
        this.isTrue = isTrue;
    }

    public SparkSQLExpression getExpr() {
        return expr;
    }

    public List<SparkSQLExpression> getListElements() {
        return listElements;
    }

    @Override
    public SparkSQLConstant getExpectedValue() {
        SparkSQLConstant leftValue = expr.getExpectedValue();
        if (leftValue == null) {
            return null;
        }
        if (leftValue.isNull()) {
            return SparkSQLConstant.createNullConstant();
        }
        boolean isNull = false;
        for (SparkSQLExpression expr : getListElements()) {
            SparkSQLConstant rightExpectedValue = expr.getExpectedValue();
            if (rightExpectedValue == null) {
                return null;
            }
            if (rightExpectedValue.isNull()) {
                isNull = true;
            } else if (rightExpectedValue.isEquals(this.expr.getExpectedValue()).isBoolean()
                    && rightExpectedValue.isEquals(this.expr.getExpectedValue()).asBoolean()) {
                return SparkSQLConstant.createBooleanConstant(isTrue);
            }
        }

        if (isNull) {
            return SparkSQLConstant.createNullConstant();
        } else {
            return SparkSQLConstant.createBooleanConstant(!isTrue);
        }
    }

    public boolean isTrue() {
        return isTrue;
    }

    @Override
    public SparkSQLDataType getExpressionType() {
        return SparkSQLDataType.BOOLEAN;
    }
}