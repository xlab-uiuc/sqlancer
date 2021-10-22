package sqlancer.sparksql.ast;

import java.util.function.BinaryOperator;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;
import sqlancer.sparksql.ast.SparkSQLBinaryArithmeticOperation.SparkSQLBinaryOperator;

public class SparkSQLBinaryArithmeticOperation extends BinaryOperatorNode<SparkSQLExpression, SparkSQLBinaryOperator>
        implements SparkSQLExpression {

    public enum SparkSQLBinaryOperator implements Operator {

        ADDITION("+") {
            @Override
            public SparkSQLConstant apply(SparkSQLConstant left, SparkSQLConstant right) {
                return applyBitOperation(left, right, (l, r) -> l + r);
            }

        },
        SUBTRACTION("-") {
            @Override
            public SparkSQLConstant apply(SparkSQLConstant left, SparkSQLConstant right) {
                return applyBitOperation(left, right, (l, r) -> l - r);
            }
        },
        MULTIPLICATION("*") {
            @Override
            public SparkSQLConstant apply(SparkSQLConstant left, SparkSQLConstant right) {
                return applyBitOperation(left, right, (l, r) -> l * r);
            }
        },
        DIVISION("/") {

            @Override
            public SparkSQLConstant apply(SparkSQLConstant left, SparkSQLConstant right) {
                return applyBitOperation(left, right, (l, r) -> r == 0 ? -1 : l / r);

            }

        },
        MODULO("%") {
            @Override
            public SparkSQLConstant apply(SparkSQLConstant left, SparkSQLConstant right) {
                return applyBitOperation(left, right, (l, r) -> r == 0 ? -1 : l % r);

            }
        },
        EXPONENTIATION("^") {
            @Override
            public SparkSQLConstant apply(SparkSQLConstant left, SparkSQLConstant right) {
                return null;
            }
        };

        private String textRepresentation;

        private static SparkSQLConstant applyBitOperation(SparkSQLConstant left, SparkSQLConstant right,
                BinaryOperator<Long> op) {
            if (left.isNull() || right.isNull()) {
                return SparkSQLConstant.createNullConstant();
            } else {
                long leftVal = left.cast(SparkSQLDataType.INT).asInt();
                long rightVal = right.cast(SparkSQLDataType.INT).asInt();
                long value = op.apply(leftVal, rightVal);
                return SparkSQLConstant.createIntConstant(value);
            }
        }

        SparkSQLBinaryOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

        public abstract SparkSQLConstant apply(SparkSQLConstant left, SparkSQLConstant right);

        public static SparkSQLBinaryOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public SparkSQLBinaryArithmeticOperation(SparkSQLExpression left, SparkSQLExpression right,
            SparkSQLBinaryOperator op) {
        super(left, right, op);
    }

    @Override
    public SparkSQLConstant getExpectedValue() {
        SparkSQLConstant leftExpected = getLeft().getExpectedValue();
        SparkSQLConstant rightExpected = getRight().getExpectedValue();
        if (leftExpected == null || rightExpected == null) {
            return null;
        }
        return getOp().apply(leftExpected, rightExpected);
    }

    @Override
    public SparkSQLDataType getExpressionType() {
        return SparkSQLDataType.INT;
    }

}