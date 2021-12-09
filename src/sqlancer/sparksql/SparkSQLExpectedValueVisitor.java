package sqlancer.sparksql;

import sqlancer.sparksql.ast.SparkSQLAggregate;
import sqlancer.sparksql.ast.SparkSQLBinaryLogicalOperation;
import sqlancer.sparksql.ast.SparkSQLCastOperation;
import sqlancer.sparksql.ast.SparkSQLColumnValue;
import sqlancer.sparksql.ast.SparkSQLConstant;
import sqlancer.sparksql.ast.SparkSQLExpression;
import sqlancer.sparksql.ast.SparkSQLInOperation;
import sqlancer.sparksql.ast.SparkSQLLikeOperation;
import sqlancer.sparksql.ast.SparkSQLOrderByTerm;
import sqlancer.sparksql.ast.SparkSQLPostfixOperation;
import sqlancer.sparksql.ast.SparkSQLPrefixOperation;
import sqlancer.sparksql.ast.SparkSQLSelect;
import sqlancer.sparksql.ast.SparkSQLSelect.SparkSQLFromTable;
import sqlancer.sparksql.ast.SparkSQLSelect.SparkSQLSubquery;

public final class SparkSQLExpectedValueVisitor implements SparkSQLVisitor {

    private final StringBuilder sb = new StringBuilder();
    private static final int NR_TABS = 0;

    private void print(SparkSQLExpression expr) {
        SparkSQLToStringVisitor v = new SparkSQLToStringVisitor();
        v.visit(expr);
        for (int i = 0; i < NR_TABS; i++) {
            sb.append("\t");
        }
        sb.append(v.get());
        sb.append(" -- ");
        sb.append(expr.getExpectedValue());
        sb.append("\n");
    }

    @Override
    public void visit(SparkSQLConstant constant) {
        print(constant);
    }

    @Override
    public void visit(SparkSQLPostfixOperation op) {
        print(op);
        visit(op.getExpression());
    }

    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(SparkSQLColumnValue c) {
        print(c);
    }

    @Override
    public void visit(SparkSQLPrefixOperation op) {
        print(op);
        visit(op.getExpression());
    }

    @Override
    public void visit(SparkSQLSelect op) {
        visit(op.getWhereClause());
    }

    @Override
    public void visit(SparkSQLOrderByTerm op) {

    }

    @Override
    public void visit(SparkSQLCastOperation cast) {
        print(cast);
        visit(cast.getExpression());
    }

    @Override
    public void visit(SparkSQLInOperation op) {
        print(op);
        visit(op.getExpr());
        for (SparkSQLExpression right : op.getListElements()) {
            visit(right);
        }
    }

    @Override
    public void visit(SparkSQLAggregate op) {
        print(op);
        for (SparkSQLExpression expr : op.getArgs()) {
            visit(expr);
        }
    }

    @Override
    public void visit(SparkSQLFromTable from) {
        print(from);
    }

    @Override
    public void visit(SparkSQLSubquery subquery) {
        print(subquery);
    }

    @Override
    public void visit(SparkSQLBinaryLogicalOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(SparkSQLLikeOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

}
