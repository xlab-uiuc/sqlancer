package sqlancer.sparksql;

import java.util.Optional;

import sqlancer.Randomly;
import sqlancer.common.visitor.BinaryOperation;
import sqlancer.common.visitor.ToStringVisitor;
import sqlancer.sparksql.ast.SparkSQLAggregate;
import sqlancer.sparksql.ast.SparkSQLBinaryLogicalOperation;
import sqlancer.sparksql.ast.SparkSQLCastOperation;
import sqlancer.sparksql.ast.SparkSQLColumnValue;
import sqlancer.sparksql.ast.SparkSQLConstant;
import sqlancer.sparksql.ast.SparkSQLExpression;
import sqlancer.sparksql.ast.SparkSQLInOperation;
import sqlancer.sparksql.ast.SparkSQLJoin;
import sqlancer.sparksql.ast.SparkSQLJoin.SparkSQLJoinType;
import sqlancer.sparksql.ast.SparkSQLLikeOperation;
import sqlancer.sparksql.ast.SparkSQLOrderByTerm;
import sqlancer.sparksql.ast.SparkSQLPostfixOperation;
import sqlancer.sparksql.ast.SparkSQLPrefixOperation;
import sqlancer.sparksql.ast.SparkSQLSelect;
import sqlancer.sparksql.ast.SparkSQLSelect.SparkSQLFromTable;
import sqlancer.sparksql.ast.SparkSQLSelect.SparkSQLSubquery;

public final class SparkSQLToStringVisitor extends ToStringVisitor<SparkSQLExpression> implements SparkSQLVisitor {

    @Override
    public void visitSpecific(SparkSQLExpression expr) {
        SparkSQLVisitor.super.visit(expr);
    }

    @Override
    public void visit(SparkSQLConstant constant) {
        sb.append(constant.toString());
    }

    @Override
    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(SparkSQLPostfixOperation op) {
        sb.append("(");
        visit(op.getExpression());
        sb.append(")");
        sb.append(" ");
        sb.append(op.getOperatorTextRepresentation());
    }

    @Override
    public void visit(SparkSQLColumnValue c) {
        sb.append(c.getColumn().getFullQualifiedName());
    }

    @Override
    public void visit(SparkSQLPrefixOperation op) {
        sb.append(op.getTextRepresentation());
        sb.append(" (");
        visit(op.getExpression());
        sb.append(")");
    }

    @Override
    public void visit(SparkSQLFromTable from) {
        sb.append(from.getTable().getName());
        if (!from.isOnly() && Randomly.getBoolean()) {
            sb.append("*");
        }
    }

    @Override
    public void visit(SparkSQLSubquery subquery) {
        sb.append("(");
        visit(subquery.getSelect());
        sb.append(") AS ");
        sb.append(subquery.getName());
    }

    @Override
    public void visit(SparkSQLSelect s) {
        sb.append("SELECT ");
        switch (s.getSelectOption()) {
            case DISTINCT:
                sb.append("DISTINCT ");
                if (s.getDistinctOnClause() != null) {
                    sb.append("ON (");
                    visit(s.getDistinctOnClause());
                    sb.append(") ");
                }
                break;
            case ALL:
                sb.append(Randomly.fromOptions("ALL ", ""));
                break;
            default:
                throw new AssertionError();
        }
        if (s.getFetchColumns() == null) {
            sb.append("*");
        } else {
            visit(s.getFetchColumns());
        }
        sb.append(" FROM ");
        visit(s.getFromList());

        for (SparkSQLJoin j : s.getJoinClauses()) {
            sb.append(" ");
            switch (j.getType()) {
                case INNER:
                    if (Randomly.getBoolean()) {
                        sb.append("INNER ");
                    }
                    sb.append("JOIN");
                    break;
                case LEFT:
                    sb.append("LEFT OUTER JOIN");
                    break;
                case RIGHT:
                    sb.append("RIGHT OUTER JOIN");
                    break;
                case FULL:
                    sb.append("FULL OUTER JOIN");
                    break;
                case CROSS:
                    sb.append("CROSS JOIN");
                    break;
                default:
                    throw new AssertionError(j.getType());
            }
            sb.append(" ");
            visit(j.getTableReference());
            if (j.getType() != SparkSQLJoinType.CROSS) {
                sb.append(" ON ");
                visit(j.getOnClause());
            }
        }

        if (s.getWhereClause() != null) {
            sb.append(" WHERE ");
            visit(s.getWhereClause());
        }
        if (s.getGroupByExpressions().size() > 0) {
            sb.append(" GROUP BY ");
            visit(s.getGroupByExpressions());
        }
        if (s.getHavingClause() != null) {
            sb.append(" HAVING ");
            visit(s.getHavingClause());

        }
        if (!s.getOrderByExpressions().isEmpty()) {
            sb.append(" ORDER BY ");
            visit(s.getOrderByExpressions());
        }
        if (s.getLimitClause() != null) {
            sb.append(" LIMIT ");
            visit(s.getLimitClause());
        }

        if (s.getOffsetClause() != null) {
            sb.append(" OFFSET ");
            visit(s.getOffsetClause());
        }
    }

    @Override
    public void visit(SparkSQLOrderByTerm op) {
        visit(op.getExpr());
        sb.append(" ");
        sb.append(op.getOrder());
    }

//    @Override
//    public void visit(SparkSQLFunction f) {
//        sb.append(f.getFunctionName());
//        sb.append("(");
//        int i = 0;
//        for (SparkSQLExpression arg : f.getArguments()) {
//            if (i++ != 0) {
//                sb.append(", ");
//            }
//            visit(arg);
//        }
//        sb.append(")");
//    }

    @Override
    public void visit(SparkSQLCastOperation cast) {
        sb.append("CAST(");
        visit(cast.getExpression());
        sb.append(" AS ");
        appendType(cast);
        sb.append(")");
    }

    private void appendType(SparkSQLCastOperation cast) {
        SparkSQLCompoundDataType compoundType = cast.getCompoundType();
        switch (compoundType.getDataType()) {
            case BOOLEAN:
                sb.append("BOOLEAN");
                break;
            case INT:
                sb.append("INT");
                break;
//            case TEXT:
//                sb.append(Randomly.fromOptions("VARCHAR"));
//                break;
//            case REAL:
//                sb.append("FLOAT");
//                break;
            case DECIMAL:
                sb.append("DECIMAL");
                break;
            case FLOAT:
                sb.append("FLOAT");
                break;
//            case BIT:
//                sb.append("BIT");
//                // if (Randomly.getBoolean()) {
//                // sb.append("(");
//                // sb.append(Randomly.getNotCachedInteger(1, 100));
//                // sb.append(")");
//                // }
//                break;
            case STRING:
                sb.append("STRING");
                break;
            default:
                throw new AssertionError(cast.getExpressionType());
        }
        Optional<Integer> size = compoundType.getSize();
        if (size.isPresent()) {
            sb.append("(");
            sb.append(size.get());
            sb.append(")");
        }
    }

    @Override
    public void visit(SparkSQLInOperation op) {
        sb.append("(");
        visit(op.getExpr());
        sb.append(")");
        if (!op.isTrue()) {
            sb.append(" NOT");
        }
        sb.append(" IN (");
        visit(op.getListElements());
        sb.append(")");
    }

//    @Override
//    public void visit(SparkSQLPostfixText op) {
//        visit(op.getExpr());
//        sb.append(op.getText());
//    }

    @Override
    public void visit(SparkSQLAggregate op) {
        sb.append(op.getFunction());
        sb.append("(");
        visit(op.getArgs());
        sb.append(")");
    }

    @Override
    public void visit(SparkSQLBinaryLogicalOperation op) {
        super.visit((BinaryOperation<SparkSQLExpression>) op);
    }

    @Override
    public void visit(SparkSQLLikeOperation op) {
        super.visit((BinaryOperation<SparkSQLExpression>) op);
    }

}
