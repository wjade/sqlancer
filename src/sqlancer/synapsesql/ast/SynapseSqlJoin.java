package sqlancer.synapsesql.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.synapsesql.SynapseSqlProvider.SynapseSqlGlobalState;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlColumn;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlTable;
import sqlancer.synapsesql.gen.SynapseSqlExpressionGenerator;

public class SynapseSqlJoin implements Node<SynapseSqlExpression> {

    private final TableReferenceNode<SynapseSqlExpression, SynapseSqlTable> leftTable;
    private final TableReferenceNode<SynapseSqlExpression, SynapseSqlTable> rightTable;
    private final JoinType joinType;
    private final Node<SynapseSqlExpression> onCondition;
    private OuterType outerType;

    public enum JoinType {
        INNER, NATURAL, LEFT, RIGHT;

        public static JoinType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public enum OuterType {
        FULL, LEFT, RIGHT;

        public static OuterType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public SynapseSqlJoin(TableReferenceNode<SynapseSqlExpression, SynapseSqlTable> leftTable,
            TableReferenceNode<SynapseSqlExpression, SynapseSqlTable> rightTable, JoinType joinType,
            Node<SynapseSqlExpression> whereCondition) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.joinType = joinType;
        this.onCondition = whereCondition;
    }

    public TableReferenceNode<SynapseSqlExpression, SynapseSqlTable> getLeftTable() {
        return leftTable;
    }

    public TableReferenceNode<SynapseSqlExpression, SynapseSqlTable> getRightTable() {
        return rightTable;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public Node<SynapseSqlExpression> getOnCondition() {
        return onCondition;
    }

    private void setOuterType(OuterType outerType) {
        this.outerType = outerType;
    }

    public OuterType getOuterType() {
        return outerType;
    }

    public static List<Node<SynapseSqlExpression>> getJoins(
            List<TableReferenceNode<SynapseSqlExpression, SynapseSqlTable>> tableList, SynapseSqlGlobalState globalState) {
        List<Node<SynapseSqlExpression>> joinExpressions = new ArrayList<>();
        while (tableList.size() >= 2 && Randomly.getBooleanWithRatherLowProbability()) {
            TableReferenceNode<SynapseSqlExpression, SynapseSqlTable> leftTable = tableList.remove(0);
            TableReferenceNode<SynapseSqlExpression, SynapseSqlTable> rightTable = tableList.remove(0);
            List<SynapseSqlColumn> columns = new ArrayList<>(leftTable.getTable().getColumns());
            columns.addAll(rightTable.getTable().getColumns());
            SynapseSqlExpressionGenerator joinGen = new SynapseSqlExpressionGenerator(globalState).setColumns(columns);
            switch (SynapseSqlJoin.JoinType.getRandom()) {
            case INNER:
                joinExpressions.add(SynapseSqlJoin.createInnerJoin(leftTable, rightTable, joinGen.generateExpression()));
                break;
            case NATURAL:
                joinExpressions.add(SynapseSqlJoin.createNaturalJoin(leftTable, rightTable, OuterType.getRandom()));
                break;
            case LEFT:
                joinExpressions
                        .add(SynapseSqlJoin.createLeftOuterJoin(leftTable, rightTable, joinGen.generateExpression()));
                break;
            case RIGHT:
                joinExpressions
                        .add(SynapseSqlJoin.createRightOuterJoin(leftTable, rightTable, joinGen.generateExpression()));
                break;
            default:
                throw new AssertionError();
            }
        }
        return joinExpressions;
    }

    public static SynapseSqlJoin createRightOuterJoin(TableReferenceNode<SynapseSqlExpression, SynapseSqlTable> left,
            TableReferenceNode<SynapseSqlExpression, SynapseSqlTable> right, Node<SynapseSqlExpression> predicate) {
        return new SynapseSqlJoin(left, right, JoinType.RIGHT, predicate);
    }

    public static SynapseSqlJoin createLeftOuterJoin(TableReferenceNode<SynapseSqlExpression, SynapseSqlTable> left,
            TableReferenceNode<SynapseSqlExpression, SynapseSqlTable> right, Node<SynapseSqlExpression> predicate) {
        return new SynapseSqlJoin(left, right, JoinType.LEFT, predicate);
    }

    public static SynapseSqlJoin createInnerJoin(TableReferenceNode<SynapseSqlExpression, SynapseSqlTable> left,
            TableReferenceNode<SynapseSqlExpression, SynapseSqlTable> right, Node<SynapseSqlExpression> predicate) {
        return new SynapseSqlJoin(left, right, JoinType.INNER, predicate);
    }

    public static Node<SynapseSqlExpression> createNaturalJoin(TableReferenceNode<SynapseSqlExpression, SynapseSqlTable> left,
            TableReferenceNode<SynapseSqlExpression, SynapseSqlTable> right, OuterType naturalJoinType) {
        SynapseSqlJoin join = new SynapseSqlJoin(left, right, JoinType.NATURAL, null);
        join.setOuterType(naturalJoinType);
        return join;
    }

}
