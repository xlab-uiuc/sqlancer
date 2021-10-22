package sqlancer.sparksql.ast;

import sqlancer.Randomly;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;

public class SparkSQLOrderByTerm implements SparkSQLExpression {

    private final SparkSQLOrder order;
    private final SparkSQLNullsOrder nulls;
    private final SparkSQLExpression expr;

    public enum SparkSQLOrder {
        ASC, DESC;

        public static SparkSQLOrder getRandomOrder() {
            return Randomly.fromOptions(SparkSQLOrder.values());
        }
    }

    public enum SparkSQLNullsOrder {
        NULLS_FIRST("NULLS FIRST"), NULLS_LAST("NULLS LAST");

        private final String textRepresentation;

        private SparkSQLNullsOrder(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public static SparkSQLNullsOrder getRandomOrder() {
            return Randomly.fromOptions(SparkSQLNullsOrder.values());
        }
    }

    public SparkSQLOrderByTerm(SparkSQLExpression expr, SparkSQLOrder order, SparkSQLNullsOrder nulls) {
        this.expr = expr;
        this.order = order;
        this.nulls = nulls;
    }

    public SparkSQLOrder getOrder() {
        return order;
    }

    public SparkSQLNullsOrder getNullsOrder() {
        return nulls;
    }

    public SparkSQLExpression getExpr() {
        return expr;
    }

    @Override
    public SparkSQLConstant getExpectedValue() {
        throw new AssertionError(this);
    }

    @Override
    public SparkSQLDataType getExpressionType() {
        return null;
    }

}