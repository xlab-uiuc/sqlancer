package sqlancer.sparksql.ast;

import sqlancer.common.visitor.UnaryOperation;

public class SparkSQLAlias implements UnaryOperation<SparkSQLExpression>, SparkSQLExpression {

    private final SparkSQLExpression expr;
    private final String alias;

    public SparkSQLAlias(SparkSQLExpression expr, String alias) {
        this.expr = expr;
        this.alias = alias;
    }

    @Override
    public SparkSQLExpression getExpression() {
        return expr;
    }

    @Override
    public String getOperatorRepresentation() {
        return " as " + alias;
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.POSTFIX;
    }

    @Override
    public boolean omitBracketsWhenPrinting() {
        return true;
    }

}