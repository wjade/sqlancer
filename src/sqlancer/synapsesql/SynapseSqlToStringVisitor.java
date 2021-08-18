package sqlancer.synapsesql;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.common.ast.newast.Node;
import sqlancer.synapsesql.ast.SynapseSqlConstant;
import sqlancer.synapsesql.ast.SynapseSqlExpression;
import sqlancer.synapsesql.ast.SynapseSqlJoin;
import sqlancer.synapsesql.ast.SynapseSqlSelect;

public class SynapseSqlToStringVisitor extends NewToStringVisitor<SynapseSqlExpression> {

    @Override
    public void visitSpecific(Node<SynapseSqlExpression> expr) {
        if (expr instanceof SynapseSqlConstant) {
            visit((SynapseSqlConstant) expr);
        } else if (expr instanceof SynapseSqlSelect) {
            visit((SynapseSqlSelect) expr);
        } else if (expr instanceof SynapseSqlJoin) {
            visit((SynapseSqlJoin) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    private void visit(SynapseSqlJoin join) {
        visit(join.getLeftTable());
        sb.append(" ");
        sb.append(join.getJoinType());
        sb.append(" ");
        if (join.getOuterType() != null) {
            sb.append(join.getOuterType());
        }
        sb.append(" JOIN ");
        visit(join.getRightTable());
        if (join.getOnCondition() != null) {
            sb.append(" ON ");
            visit(join.getOnCondition());
        }
    }

    private void visit(SynapseSqlConstant constant) {
        sb.append(constant.toString());
    }

    private void visit(SynapseSqlSelect select) {
        sb.append("SELECT ");
        if (select.isDistinct()) {
            sb.append("DISTINCT ");
        }
        visit(select.getFetchColumns());
        sb.append(" FROM ");
        visit(select.getFromList());
        if (!select.getFromList().isEmpty() && !select.getJoinList().isEmpty()) {
            sb.append(", ");
        }
        if (!select.getJoinList().isEmpty()) {
            visit(select.getJoinList());
        }
        if (select.getWhereClause() != null) {
            sb.append(" WHERE ");
            visit(select.getWhereClause());
        }
        if (!select.getGroupByExpressions().isEmpty()) {
            sb.append(" GROUP BY ");
            visit(select.getGroupByExpressions());
        }
        if (select.getHavingClause() != null) {
            sb.append(" HAVING ");
            visit(select.getHavingClause());
        }
        if (!select.getOrderByExpressions().isEmpty()) {
            sb.append(" ORDER BY ");
            visit(select.getOrderByExpressions());
        }
        if (select.getLimitClause() != null) {
            sb.append(" LIMIT ");
            visit(select.getLimitClause());
        }
        if (select.getOffsetClause() != null) {
            sb.append(" OFFSET ");
            visit(select.getOffsetClause());
        }
    }

    public static String asString(Node<SynapseSqlExpression> expr) {
        SynapseSqlToStringVisitor visitor = new SynapseSqlToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

}
