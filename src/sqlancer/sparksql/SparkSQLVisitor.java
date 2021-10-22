package sqlancer.sparksql;

import java.util.List;

import sqlancer.sparksql.SparkSQLSchema.SparkSQLColumn;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;
//import sqlancer.sparksql.ast.SparkSQLAggregate;
//import sqlancer.sparksql.ast.SparkSQLBetweenOperation;
//import sqlancer.sparksql.ast.SparkSQLBinaryLogicalOperation;
//import sqlancer.sparksql.ast.SparkSQLCastOperation;
//import sqlancer.sparksql.ast.SparkSQLCollate;
//import sqlancer.sparksql.ast.SparkSQLColumnValue;
import sqlancer.sparksql.ast.SparkSQLConstant;
import sqlancer.sparksql.ast.SparkSQLExpression;
//import sqlancer.sparksql.ast.SparkSQLFunction;
//import sqlancer.sparksql.ast.SparkSQLInOperation;
//import sqlancer.sparksql.ast.SparkSQLLikeOperation;
//import sqlancer.sparksql.ast.SparkSQLOrderByTerm;
//import sqlancer.sparksql.ast.SparkSQLPOSIXRegularExpression;
//import sqlancer.sparksql.ast.SparkSQLPostfixOperation;
//import sqlancer.sparksql.ast.SparkSQLPostfixText;
//import sqlancer.sparksql.ast.SparkSQLPrefixOperation;
//import sqlancer.sparksql.ast.SparkSQLSelect;
//import sqlancer.sparksql.ast.SparkSQLSelect.SparkSQLFromTable;
//import sqlancer.sparksql.ast.SparkSQLSelect.SparkSQLSubquery;
//import sqlancer.sparksql.ast.SparkSQLSimilarTo;
import sqlancer.sparksql.gen.SparkSQLExpressionGenerator;
import sqlancer.sparksql.SparkSQLProvider.SparkSQLGlobalState;

public interface SparkSQLVisitor {

    void visit(SparkSQLConstant constant);

//    void visit(SparkSQLPostfixOperation op);
//
//    void visit(SparkSQLColumnValue c);
//
//    void visit(SparkSQLPrefixOperation op);
//
//    void visit(SparkSQLSelect op);
//
//    void visit(SparkSQLOrderByTerm op);
//
//    void visit(SparkSQLFunction f);
//
//    void visit(SparkSQLCastOperation cast);
//
//    void visit(SparkSQLBetweenOperation op);
//
//    void visit(SparkSQLInOperation op);
//
//    void visit(SparkSQLPostfixText op);
//
//    void visit(SparkSQLAggregate op);
//
//    void visit(SparkSQLSimilarTo op);
//
//    void visit(SparkSQLCollate op);
//
//    void visit(SparkSQLPOSIXRegularExpression op);
//
//    void visit(SparkSQLFromTable from);
//
//    void visit(SparkSQLSubquery subquery);
//
//    void visit(SparkSQLBinaryLogicalOperation op);
//
//    void visit(SparkSQLLikeOperation op);

    default void visit(SparkSQLExpression expression) {
        if (expression instanceof SparkSQLConstant) {
            visit((SparkSQLConstant) expression);
//        } else if (expression instanceof SparkSQLPostfixOperation) {
//            visit((SparkSQLPostfixOperation) expression);
//        } else if (expression instanceof SparkSQLColumnValue) {
//            visit((SparkSQLColumnValue) expression);
//        } else if (expression instanceof SparkSQLPrefixOperation) {
//            visit((SparkSQLPrefixOperation) expression);
//        } else if (expression instanceof SparkSQLSelect) {
//            visit((SparkSQLSelect) expression);
//        } else if (expression instanceof SparkSQLOrderByTerm) {
//            visit((SparkSQLOrderByTerm) expression);
//        } else if (expression instanceof SparkSQLFunction) {
//            visit((SparkSQLFunction) expression);
//        } else if (expression instanceof SparkSQLCastOperation) {
//            visit((SparkSQLCastOperation) expression);
//        } else if (expression instanceof SparkSQLBetweenOperation) {
//            visit((SparkSQLBetweenOperation) expression);
//        } else if (expression instanceof SparkSQLInOperation) {
//            visit((SparkSQLInOperation) expression);
//        } else if (expression instanceof SparkSQLAggregate) {
//            visit((SparkSQLAggregate) expression);
//        } else if (expression instanceof SparkSQLPostfixText) {
//            visit((SparkSQLPostfixText) expression);
//        } else if (expression instanceof SparkSQLSimilarTo) {
//            visit((SparkSQLSimilarTo) expression);
//        } else if (expression instanceof SparkSQLPOSIXRegularExpression) {
//            visit((SparkSQLPOSIXRegularExpression) expression);
//        } else if (expression instanceof SparkSQLCollate) {
//            visit((SparkSQLCollate) expression);
//        } else if (expression instanceof SparkSQLFromTable) {
//            visit((SparkSQLFromTable) expression);
//        } else if (expression instanceof SparkSQLSubquery) {
//            visit((SparkSQLSubquery) expression);
//        } else if (expression instanceof SparkSQLLikeOperation) {
//            visit((SparkSQLLikeOperation) expression);
        } else {
            throw new AssertionError(expression);
        }
    }

    static String asString(SparkSQLExpression expr) {
        SparkSQLToStringVisitor visitor = new SparkSQLToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

//    static String asExpectedValues(SparkSQLExpression expr) {
//        SparkSQLExpectedValueVisitor v = new SparkSQLExpectedValueVisitor();
//        v.visit(expr);
//        return v.get();
//    }

    static String getExpressionAsString(SparkSQLGlobalState globalState, SparkSQLDataType type,
                                        List<SparkSQLColumn> columns) {
        SparkSQLExpression expression = SparkSQLExpressionGenerator.generateExpression(globalState, columns, type);
        SparkSQLToStringVisitor visitor = new SparkSQLToStringVisitor();
        visitor.visit(expression);
        return visitor.get();
    }

}
