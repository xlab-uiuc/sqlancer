package sqlancer.sparksql.ast;

import java.util.Collections;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.SelectBase;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLTable;

public class SparkSQLSelect extends SelectBase<SparkSQLExpression> implements SparkSQLExpression {

    private SelectType selectOption = SelectType.ALL;
    private List<SparkSQLJoin> joinClauses = Collections.emptyList();
    private SparkSQLExpression distinctOnClause;

    public static class SparkSQLFromTable implements SparkSQLExpression {
        private final SparkSQLTable t;

        // TODO enhance FROM modifier
        public SparkSQLFromTable(SparkSQLTable t) {
            this.t = t;
        }

        public SparkSQLTable getTable() {
            return t;
        }

        @Override
        public SparkSQLDataType getExpressionType() {
            return null;
        }
    }

    public static class SparkSQLSubquery implements SparkSQLExpression {
        private final SparkSQLSelect s;
        private final String name;

        public SparkSQLSubquery(SparkSQLSelect s, String name) {
            this.s = s;
            this.name = name;
        }

        public SparkSQLSelect getSelect() {
            return s;
        }

        public String getName() {
            return name;
        }

        @Override
        public SparkSQLDataType getExpressionType() {
            return null;
        }
    }

    public enum SelectType {
        DISTINCT, ALL;

        public static SelectType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public void setSelectType(SelectType fromOptions) {
        this.setSelectOption(fromOptions);
    }

    public void setDistinctOnClause(SparkSQLExpression distinctOnClause) {
        if (selectOption != SelectType.DISTINCT) {
            throw new IllegalArgumentException();
        }
        this.distinctOnClause = distinctOnClause;
    }

    public SelectType getSelectOption() {
        return selectOption;
    }

    public void setSelectOption(SelectType fromOptions) {
        this.selectOption = fromOptions;
    }

    @Override
    public SparkSQLDataType getExpressionType() {
        return null;
    }

    public void setJoinClauses(List<SparkSQLJoin> joinStatements) {
        this.joinClauses = joinStatements;

    }

    public List<SparkSQLJoin> getJoinClauses() {
        return joinClauses;
    }

    public SparkSQLExpression getDistinctOnClause() {
        return distinctOnClause;
    }
    
}