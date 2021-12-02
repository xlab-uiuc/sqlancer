package sqlancer.sparksql.gen;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.sparksql.SparkSQLCompoundDataType;
import sqlancer.sparksql.SparkSQLSchema;
import sqlancer.sparksql.ast.SparkSQLBinaryComparisonOperation;
import sqlancer.sparksql.ast.SparkSQLExpression;
import sqlancer.sparksql.ast.SparkSQLInOperation;
import sqlancer.sparksql.ast.*;
import sqlancer.sparksql.ast.SparkSQLBinaryArithmeticOperation;
import sqlancer.sparksql.ast.SparkSQLCastOperation;
import sqlancer.sparksql.ast.SparkSQLPrefixOperation;
import sqlancer.sparksql.SparkSQLProvider.SparkSQLGlobalState;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLColumn;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLRowValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class SparkSQLExpressionGenerator implements ExpressionGenerator<SparkSQLExpression> {

    private final SparkSQLGlobalState state;
    private final Randomly r;
    private final int maxDepth;
    private SparkSQLRowValue rw;
    private List<SparkSQLColumn> columns;

    public SparkSQLExpressionGenerator(SparkSQLGlobalState state) {
        this.state = state;
        this.r = state.getRandomly();
        this.maxDepth = state.getOptions().getMaxExpressionDepth();
    }

    private enum Actions {

    }

    public SparkSQLExpressionGenerator setColumns(List<SparkSQLColumn> columns) {
        this.columns = columns;
        return this;
    }

//    public SparkSQLExpressionGenerator setRowValue(SparkSQLSchema.SparkSQLRowValue rw) {
//        this.rw = rw;
//        return this;
//    }

    public SparkSQLExpression generateExpression(int depth) {
        return generateExpression(depth, SparkSQLDataType.getRandomType());
    }

    public static SparkSQLExpression generateExpression(SparkSQLGlobalState globalState,
                                                        SparkSQLDataType type) {
        return new SparkSQLExpressionGenerator(globalState).generateExpression(0, type);
    }

    public SparkSQLExpression generateExpression(int depth, SparkSQLDataType originalType) {
        SparkSQLDataType dataType = originalType;
        if (dataType == SparkSQLDataType.FLOAT && Randomly.getBoolean()) {
            dataType = SparkSQLDataType.INT;
        }
        return generateExpressionInternal(depth, dataType);
    }

    public static SparkSQLExpression generateExpression(SparkSQLGlobalState globalState, List<SparkSQLColumn> columns,
                                                        SparkSQLDataType type) {
        return new SparkSQLExpressionGenerator(globalState).setColumns(columns).generateExpression(0, type);
    }

    public static SparkSQLExpression generateExpression(SparkSQLGlobalState globalState, List<SparkSQLColumn> columns) {
        return new SparkSQLExpressionGenerator(globalState).setColumns(columns).generateExpression(0);
    }

    public SparkSQLExpression generateExpression(SparkSQLDataType dataType) {
        return generateExpression(0, dataType);
    }

    private SparkSQLExpression generateExpressionInternal(int depth, SparkSQLDataType dataType) throws AssertionError {
        // TODO: maybe consider aggregate or other built in functions
        // if (allowAggregateFunctions && Randomly.getBoolean()) {
        //     allowAggregateFunctions = false; // aggregate function calls cannot be nested
        //     return getAggregate(dataType);
        //}
        if (Randomly.getBooleanWithRatherLowProbability() || depth > maxDepth) {
            // generic expression
            //if (Randomly.getBoolean() || depth > maxDepth) {
                if (Randomly.getBooleanWithRatherLowProbability()) {
                    return generateConstant(r, dataType);
                } else {
                    if (filterColumns(dataType).isEmpty()) {
                        return generateConstant(r, dataType);
                    } else {
                        return createColumnOfType(dataType);
                    }
                }
            }
        // TODO: for built-in functions
        // else {
        //      if (Randomly.getBoolean()) {
//                    return new SparkSQLCastOperation(generateExpression(depth + 1), getCompoundDataType(dataType));
//                } else {
//                    return generateFunctionWithUnknownResult(depth, dataType);
//                }
//            }
        else {
            switch (dataType) {
                case BOOLEAN:
                    return generateBooleanExpression(depth);
                case INT:
                    return generateIntExpression(depth);
                case DECIMAL:
                case FLOAT:
                case DOUBLE:
                case STRING:
                    return generateStringExpression(depth);
//                case BINARY:
//                    return generateBinaryExpression(depth);
//                case MAP:
//                    return generateMapExpression(depth);
//                case ARRAY:
//                    return generateArrayExpression(depth);
//                case STRUCT:
//                    return generateStructExpression(depth);
                default:
                    throw new AssertionError(dataType);
            }
        }
    }

    private enum BooleanExpression {
        NOT, BINARY_LOGICAL_OPERATOR, BINARY_COMPARISON, CAST, LIKE, IN_OPERATION
    }

    private SparkSQLExpression generateBooleanExpression(int depth) {
        List<SparkSQLExpressionGenerator.BooleanExpression> validOptions = new ArrayList<>(Arrays.asList(SparkSQLExpressionGenerator.BooleanExpression.values()));
        SparkSQLExpressionGenerator.BooleanExpression option = Randomly.fromList(validOptions);
        switch (option) {
            case IN_OPERATION:
                return inOperation(depth + 1);
            case NOT:
                return new SparkSQLPrefixOperation(generateExpression(depth + 1, SparkSQLSchema.SparkSQLDataType.BOOLEAN),
                        SparkSQLPrefixOperation.PrefixOperator.NOT);
            case BINARY_LOGICAL_OPERATOR:
                SparkSQLExpression first = generateExpression(depth + 1, SparkSQLSchema.SparkSQLDataType.BOOLEAN);
                int nr = Randomly.smallNumber() + 1;
                for (int i = 0; i < nr; i++) {
                    first = new SparkSQLBinaryLogicalOperation(first,
                            generateExpression(depth + 1, SparkSQLSchema.SparkSQLDataType.BOOLEAN), SparkSQLBinaryLogicalOperation.BinaryLogicalOperator.getRandom());
                }
                return first;
            case BINARY_COMPARISON:
                SparkSQLDataType dataType = getMeaningfulType();
                return generateComparison(depth, dataType);
            case CAST:
                return new SparkSQLCastOperation(generateExpression(depth + 1), getCompoundDataType(SparkSQLDataType.BOOLEAN));
            case LIKE:
                return new SparkSQLLikeOperation(generateExpression(depth + 1, SparkSQLDataType.STRING),
                        generateExpression(depth + 1, SparkSQLDataType.STRING));
            default:
                throw new AssertionError();
        }
    }

    private static SparkSQLCompoundDataType getCompoundDataType(SparkSQLSchema.SparkSQLDataType type) {
        switch (type) {
            case BOOLEAN:
            case DECIMAL: // TODO
            case FLOAT:
            case INT:
            case STRING:
            case DOUBLE:
//            case TEXT: // TODO
//            case BIT:
//                if (Randomly.getBoolean()) {
                return SparkSQLCompoundDataType.create(type);
//                } else {
//                    return SparkSQLCompoundDataType.create(type, (int) Randomly.getNotCachedInteger(1, 1000));
//                }
            default:
                throw new AssertionError(type);
        }

    }

    private SparkSQLSchema.SparkSQLDataType getMeaningfulType() {
        // make it more likely that the expression does not only consist of constant
        // expressions
        if (Randomly.getBooleanWithSmallProbability() || columns == null || columns.isEmpty()) {
            return SparkSQLSchema.SparkSQLDataType.getRandomType();
        } else {
            return Randomly.fromList(columns).getType();
        }
    }

    private SparkSQLExpression generateComparison(int depth, SparkSQLSchema.SparkSQLDataType dataType) {
        SparkSQLExpression leftExpr = generateExpression(depth + 1, dataType);
        SparkSQLExpression rightExpr = generateExpression(depth + 1, dataType);
        return getComparison(leftExpr, rightExpr);
    }

    private SparkSQLExpression getComparison(SparkSQLExpression leftExpr, SparkSQLExpression rightExpr) {
        return new SparkSQLBinaryComparisonOperation(leftExpr, rightExpr,
                SparkSQLBinaryComparisonOperation.SparkSQLBinaryComparisonOperator.getRandom());
    }

    private SparkSQLExpression inOperation(int depth) {
        SparkSQLSchema.SparkSQLDataType type = SparkSQLSchema.SparkSQLDataType.getRandomType();
        SparkSQLExpression leftExpr = generateExpression(depth + 1, type);
        List<SparkSQLExpression> rightExpr = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            rightExpr.add(generateExpression(depth + 1, type));
        }
        return new SparkSQLInOperation(leftExpr, rightExpr, Randomly.getBoolean());
    }

    private enum IntExpression {
        UNARY_OPERATION, CAST, BINARY_ARITHMETIC_EXPRESSION
        // TODO: add built-in function
    }

    private SparkSQLExpression generateIntExpression(int depth) {
        IntExpression option;
        option = Randomly.fromOptions(IntExpression.values());
        switch (option) {
            case CAST:
                return new SparkSQLCastOperation(generateExpression(depth + 1), getCompoundDataType(SparkSQLDataType.INT));
            case UNARY_OPERATION:
                SparkSQLExpression intExpression = generateExpression(depth + 1, SparkSQLDataType.INT);
                return new SparkSQLPrefixOperation(intExpression,
                        Randomly.getBoolean() ? SparkSQLPrefixOperation.PrefixOperator.UNARY_PLUS : SparkSQLPrefixOperation.PrefixOperator.UNARY_MINUS);
            //case FUNCTION:
            //    return generateFunction(depth + 1, SparkSQLSchema.SparkSQLDataType.INT);
            case BINARY_ARITHMETIC_EXPRESSION:
                return new SparkSQLBinaryArithmeticOperation(generateExpression(depth + 1, SparkSQLDataType.INT),
                        generateExpression(depth + 1, SparkSQLDataType.INT), SparkSQLBinaryArithmeticOperation.SparkSQLBinaryOperator.getRandom());
            default:
                throw new AssertionError();
        }
    }

    private enum StringExpression {
        CAST, CONCAT
    }

    private SparkSQLExpression generateStringExpression(int depth) {
        StringExpression option;
        List<StringExpression> validOptions = new ArrayList<>(Arrays.asList(StringExpression.values()));
        option = Randomly.fromList(validOptions);

        switch (option) {
            case CAST:
                return new SparkSQLCastOperation(generateExpression(depth + 1), getCompoundDataType(SparkSQLDataType.STRING));
            case CONCAT:
                return generateConcat(depth);
            default:
                throw new AssertionError();
        }
    }
    
    private SparkSQLExpression generateConcat(int depth) {
        SparkSQLExpression left = generateExpression(depth + 1, SparkSQLDataType.STRING);
        SparkSQLExpression right = generateExpression(depth + 1);
        return new SparkSQLConcatOperation(left, right);
    }

//    private enum BinaryExpression {
//        BINARY_OPERATION
//    }

//    private SparkSQLExpression generateBinaryExpression(int depth) {
//        BinaryExpression option;
//        option = Randomly.fromOptions(SparkSQLExpressionGenerator.BinaryExpression.values());
//        if (option == BinaryExpression.BINARY_OPERATION) {
//            return new SparkSQLBinaryBitOperation(SparkSQLBinaryBitOperation.SparkSQLBinaryBitOperator.getRandom(),
//                    generateExpression(depth + 1, SparkSQLDataType.BINARY),
//                    generateExpression(depth + 1, SparkSQLDataType.BINARY));
//        }
//        throw new AssertionError();
//    }

    public static SparkSQLExpression generateConstant(Randomly r, SparkSQLDataType type) {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return SparkSQLConstant.createNullConstant();
        }
        switch (type) {
            case INT:
                // TODO: why does SparkSQL have createTextConstant here?
                return SparkSQLConstant.createIntConstant(r.getInteger());
            case BOOLEAN:
                return SparkSQLConstant.createBooleanConstant(Randomly.getBoolean());
            case STRING:
                return SparkSQLConstant.createStringConstant(r.getString());
            case DECIMAL:
                return SparkSQLConstant.createDecimalConstant(r.getRandomBigDecimal());
            case FLOAT:
                return SparkSQLConstant.createFloatConstant((float) r.getDouble());
            case DOUBLE:
                return SparkSQLConstant.createDoubleConstant(r.getDouble());
//            case BINARY:
//                return SparkSQLConstant.createBinaryConstant(r.getInteger());
            default:
                throw new AssertionError(type);
        }
    }

    private SparkSQLExpression createColumnOfType(SparkSQLDataType type) {
        List<SparkSQLColumn> columns = filterColumns(type);
        SparkSQLColumn fromList = Randomly.fromList(columns);
        SparkSQLConstant value = rw == null ? null : rw.getValues().get(fromList);
        return SparkSQLColumnValue.create(fromList, value);
    }

    final List<SparkSQLColumn> filterColumns(SparkSQLDataType type) {
        if (columns == null) {
            return Collections.emptyList();
        } else {
            return columns.stream().filter(c -> c.getType() == type).collect(Collectors.toList());
        }
    }

    @Override
    public SparkSQLExpression generatePredicate() {
        return generateExpression(SparkSQLDataType.BOOLEAN);
    }

    @Override
    public SparkSQLExpression negatePredicate(SparkSQLExpression predicate) {
        return new SparkSQLPrefixOperation(predicate, SparkSQLPrefixOperation.PrefixOperator.NOT);
    }

    @Override
    public SparkSQLExpression isNull(SparkSQLExpression expr) {
        return new SparkSQLPostfixOperation(expr, SparkSQLPostfixOperation.PostfixOperator.IS_NULL);
    }

}
