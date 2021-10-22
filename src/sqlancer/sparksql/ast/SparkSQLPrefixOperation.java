package sqlancer.sparksql.ast;

import sqlancer.IgnoreMeException;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;

public class SparkSQLPrefixOperation implements SparkSQLExpression {

    public enum PrefixOperator implements Operator {
        NOT("NOT", SparkSQLDataType.BOOLEAN) {

            @Override
            public SparkSQLDataType getExpressionType() {
                return SparkSQLDataType.BOOLEAN;
            }

            @Override
            protected SparkSQLConstant getExpectedValue(SparkSQLConstant expectedValue) {
                if (expectedValue.isNull()) {
                    return SparkSQLConstant.createNullConstant();
                } else {
                    return SparkSQLConstant
                            .createBooleanConstant(!expectedValue.cast(SparkSQLDataType.BOOLEAN).asBoolean());
                }
            }
        },
        UNARY_PLUS("+", SparkSQLDataType.INT) {
            // TODO: support other data types if this is not enough
            @Override
            public SparkSQLDataType getExpressionType() {
                return SparkSQLDataType.INT;
            }

            @Override
            protected SparkSQLConstant getExpectedValue(SparkSQLConstant expectedValue) {
                // TODO: actual converts to double precision
                return expectedValue;
            }

        },
        UNARY_MINUS("-", SparkSQLDataType.INT) {
            // TODO: support other data types if this is not enough
            @Override
            public SparkSQLDataType getExpressionType() {
                return SparkSQLDataType.INT;
            }

            @Override
            protected SparkSQLConstant getExpectedValue(SparkSQLConstant expectedValue) {
                if (expectedValue.isNull()) {
                    // TODO
                    throw new IgnoreMeException();
                }
                if (expectedValue.isInt() && expectedValue.asInt() == Long.MIN_VALUE) {
                    throw new IgnoreMeException();
                }
                try {
                    return SparkSQLConstant.createIntConstant(-expectedValue.asInt());
                } catch (UnsupportedOperationException e) {
                    return null;
                }
            }

        };

        private String textRepresentation;
        private SparkSQLDataType[] dataTypes;

        PrefixOperator(String textRepresentation, SparkSQLDataType... dataTypes) {
            this.textRepresentation = textRepresentation;
            this.dataTypes = dataTypes.clone();
        }

        public abstract SparkSQLDataType getExpressionType();

        protected abstract SparkSQLConstant getExpectedValue(SparkSQLConstant expectedValue);

        @Override
        public String getTextRepresentation() {
            return toString();
        }

    }

    private final SparkSQLExpression expr;
    private final PrefixOperator op;

    public SparkSQLPrefixOperation(SparkSQLExpression expr, PrefixOperator op) {
        this.expr = expr;
        this.op = op;
    }

    @Override
    public SparkSQLDataType getExpressionType() {
        return op.getExpressionType();
    }

    @Override
    public SparkSQLConstant getExpectedValue() {
        SparkSQLConstant expectedValue = expr.getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return op.getExpectedValue(expectedValue);
    }

    public SparkSQLDataType[] getInputDataTypes() {
        return op.dataTypes;
    }

    public String getTextRepresentation() {
        return op.textRepresentation;
    }

    public SparkSQLExpression getExpression() {
        return expr;
    }

}