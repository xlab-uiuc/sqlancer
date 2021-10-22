package sqlancer.sparksql.gen;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.postgres.PostgresSchema;
import sqlancer.postgres.ast.PostgresBinaryArithmeticOperation;
import sqlancer.postgres.ast.PostgresCastOperation;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresPrefixOperation;
import sqlancer.postgres.gen.PostgresExpressionGenerator;
import sqlancer.sparksql.SparkSQLProvider.SparkSQLGlobalState;
import sqlancer.sparksql.SparkSQLSchema;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLColumn;
import sqlancer.sparksql.ast.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

// TODO: determine correct expression generator
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

    public SparkSQLExpressionGenerator setRowValue(SparkSQLSchema.SparkSQLRowValue rw) {
        this.rw = rw;
        return this;
    }

    // TODO: implement
    @Override
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
//                    return new PostgresCastOperation(generateExpression(depth + 1), getCompoundDataType(dataType));
//                } else {
//                    return generateFunctionWithUnknownResult(depth, dataType);
//                }
//            }
        //}
        else {
            switch (dataType) {
                case BOOLEAN:
                    return generateBooleanExpression(depth);
                case INT:
                    return generateIntExpression(depth);
                case DECIMAL:
                case FLOAT:
                case DOUBLE:
                case DATETIME:
                    return generateConstant(r, dataType);
                case STRING:
                    return generateStringExpression(depth);
                case BINARY:
                    return generateBinaryExpression(depth);
                case MAP:
                    return generateMapExpression(depth);
                case ARRAY:
                    return generateArrayExpression(depth);
                case STRUCT:
                    return generateStructExpression(depth);

                default:
                    throw new AssertionError(dataType);
            }
        }
    }

    // TODO: implement
    private SparkSQLExpression getExists() {
        return null;
    }

    // TODO: implement
    private SparkSQLExpression getComputableFunction(int depth) {
        return null;
    }

    // TODO: implement
    private enum ConstantType {
        INT, NULL, STRING, DOUBLE;
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
                return new SparkSQLCastOperation(generateExpression(depth + 1), SparkSQLDataType.INT);
            case UNARY_OPERATION:
                SparkSQLExpression intExpression = generateExpression(depth + 1, SparkSQLDataType.INT);
                return new SparkSQLPrefixOperation(intExpression,
                        Randomly.getBoolean() ? PostgresPrefixOperation.PrefixOperator.UNARY_PLUS : PostgresPrefixOperation.PrefixOperator.UNARY_MINUS);
            //case FUNCTION:
            //    return generateFunction(depth + 1, PostgresSchema.PostgresDataType.INT);
            case BINARY_ARITHMETIC_EXPRESSION:
                return new SparkSQLBinaryArithmeticOperation(generateExpression(depth + 1, SparkSQLDataType.INT),
                        generateExpression(depth + 1, SparkSQLDataType.INT), PostgresBinaryArithmeticOperation.PostgresBinaryOperator.getRandom());
            default:
                throw new AssertionError();
        }
    }

    public static SparkSQLExpression generateConstant(Randomly r, SparkSQLDataType type) {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return SparkSQLConstant.createNullConstant();
        }
        switch (type) {
            case INT:
                // TODO: why does Postgres have createTextConstant here?
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
            case BINARY:
                return SparkSQLConstant.createBinaryConstant(r.getInteger());
            case DATETIME:
                return SparkSQLConstant.createDatetimeConstant(r.getDatetime());
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

    // TODO: determine whether to implement
    @Override
    protected SparkSQLExpression generateColumn() {
        SparkSQLColumn c = Randomly.fromList(columns);
        return null;
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

    // TODO: implement
    @Override
    public SparkSQLExpression negatePredicate(SparkSQLExpression predicate) {
        return new SparkSQLUnaryPrefixOperation(predicate, SparkSQLUnaryPrefixOperator.NOT);
    }

    // TODO: implement
    @Override
    public SparkSQLExpression isNull(SparkSQLExpression expr) {
        return new SparkSQLUnaryPostfixOperation(expr, SparkSQLUnaryPostfixOperation.UnaryPostfixOperator.IS_NULL);
    }

}
