package sqlancer.synapsesql.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.synapsesql.SynapseSqlProvider.SynapseSqlGlobalState;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlTable;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlTables;
import sqlancer.synapsesql.ast.SynapseSqlConstant;
import sqlancer.synapsesql.ast.SynapseSqlExpression;
import sqlancer.synapsesql.ast.SynapseSqlJoin;
import sqlancer.synapsesql.ast.SynapseSqlSelect;

public final class SynapseSqlRandomQuerySynthesizer {

    private SynapseSqlRandomQuerySynthesizer() {
    }

    public static SynapseSqlSelect generateSelect(SynapseSqlGlobalState globalState, int nrColumns) {
        SynapseSqlTables targetTables = globalState.getSchema().getRandomTableNonEmptyTables();
        SynapseSqlExpressionGenerator gen = new SynapseSqlExpressionGenerator(globalState)
                .setColumns(targetTables.getColumns());
        SynapseSqlSelect select = new SynapseSqlSelect();
        // TODO: distinct
        // select.setDistinct(Randomly.getBoolean());
        // boolean allowAggregates = Randomly.getBooleanWithSmallProbability();
        List<Node<SynapseSqlExpression>> columns = new ArrayList<>();
        for (int i = 0; i < nrColumns; i++) {
            // if (allowAggregates && Randomly.getBoolean()) {
            Node<SynapseSqlExpression> expression = gen.generateExpression();
            columns.add(expression);
            // } else {
            // columns.add(gen());
            // }
        }
        select.setFetchColumns(columns);
        List<SynapseSqlTable> tables = targetTables.getTables();
        List<TableReferenceNode<SynapseSqlExpression, SynapseSqlTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<SynapseSqlExpression, SynapseSqlTable>(t)).collect(Collectors.toList());
        List<Node<SynapseSqlExpression>> joins = SynapseSqlJoin.getJoins(tableList, globalState);
        select.setJoinList(joins.stream().collect(Collectors.toList()));
        select.setFromList(tableList.stream().collect(Collectors.toList()));
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression());
        }
        if (Randomly.getBoolean()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        if (Randomly.getBoolean()) {
            select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        }

        if (Randomly.getBoolean()) {
            select.setLimitClause(SynapseSqlConstant.createIntConstant(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE)));
        }
        if (Randomly.getBoolean()) {
            select.setOffsetClause(
                    SynapseSqlConstant.createIntConstant(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE)));
        }
        if (Randomly.getBoolean()) {
            select.setHavingClause(gen.generateHavingClause());
        }
        return select;
    }

}
