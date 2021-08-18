package sqlancer.synapsesql.ast;

import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.Node;

public class SynapseSqlSelect extends SelectBase<Node<SynapseSqlExpression>> implements Node<SynapseSqlExpression> {

    private boolean isDistinct;

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

}
