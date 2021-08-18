package sqlancer.synapsesql.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.synapsesql.SynapseSqlErrors;
import sqlancer.synapsesql.SynapseSqlProvider.SynapseSqlGlobalState;
import sqlancer.synapsesql.SynapseSqlSchema;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlColumn;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlTable;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlTables;
import sqlancer.synapsesql.ast.SynapseSqlExpression;
import sqlancer.synapsesql.ast.SynapseSqlJoin;
import sqlancer.synapsesql.ast.SynapseSqlSelect;
import sqlancer.synapsesql.gen.SynapseSqlExpressionGenerator;

public class SynapseSqlQueryPartitioningBase
        extends TernaryLogicPartitioningOracleBase<Node<SynapseSqlExpression>, SynapseSqlGlobalState> implements TestOracle {

    SynapseSqlSchema s;
    SynapseSqlTables targetTables;
    SynapseSqlExpressionGenerator gen;
    SynapseSqlSelect select;

    public SynapseSqlQueryPartitioningBase(SynapseSqlGlobalState state) {
        super(state);
        SynapseSqlErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new SynapseSqlExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new SynapseSqlSelect();
        select.setFetchColumns(generateFetchColumns());
        List<SynapseSqlTable> tables = targetTables.getTables();
        List<TableReferenceNode<SynapseSqlExpression, SynapseSqlTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<SynapseSqlExpression, SynapseSqlTable>(t)).collect(Collectors.toList());
        List<Node<SynapseSqlExpression>> joins = SynapseSqlJoin.getJoins(tableList, state);
        select.setJoinList(joins.stream().collect(Collectors.toList()));
        select.setFromList(tableList.stream().collect(Collectors.toList()));
        select.setWhereClause(null);
    }

    List<Node<SynapseSqlExpression>> generateFetchColumns() {
        List<Node<SynapseSqlExpression>> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns.add(new ColumnReferenceNode<>(new SynapseSqlColumn("*", null, false, false)));
        } else {
            columns = Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                    .map(c -> new ColumnReferenceNode<SynapseSqlExpression, SynapseSqlColumn>(c)).collect(Collectors.toList());
        }
        return columns;
    }

    @Override
    protected ExpressionGenerator<Node<SynapseSqlExpression>> getGen() {
        return gen;
    }

}
