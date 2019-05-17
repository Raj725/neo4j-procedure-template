package udf;

import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

/**
 * @author RajendraK
 */

public class Transaction {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @UserFunction("udf.fn_Parent_Child_tx_Sum")
    @Description("fn_Parent_Child_tx_Sum('fromAddress','toAddress') return the total trancsactions from this address to other address")
    public Double fn_Parent_Child_tx_Sum(@Name("value") String fromAddress, @Name("value") String toAddress) {
        log.info("Getting Transactions for Address: %s", fromAddress);
        if (fromAddress == null || toAddress == null) {
            return 0.0;
        }
        Node fromAddressNode = db.findNode(Label.label("Addresses"), "blockchain_address", fromAddress);
        Node toAddressNode = db.findNode(Label.label("Addresses"), "blockchain_address", toAddress);

        if (fromAddressNode == null || toAddressNode == null) {
            return 0.0;
        }

        Iterable<Relationship> transactions = fromAddressNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("Transactions"));
        double totalAmount = 0.0;
        for (Relationship relationship : transactions) {
            if (relationship.getOtherNode(fromAddressNode).equals(toAddressNode)) {
                Object amount = relationship.getProperty("amount");
                if (amount instanceof Number) {
                    totalAmount += ((Number) amount).doubleValue();
                }
            }
        }
        return totalAmount;
    }

    @UserFunction("udf.fn_Parent_Child_tx_Sum_2")
    @Description("fn_Parent_Child_tx_Sum('fromAddressNode','toAddressNode') return the total of trancsactions from this address node to other address node")
    public Double fn_Parent_Child_tx_Sum(@Name("from") Node fromAddressNode, @Name("to") Node toAddressNode) {
        log.info("Getting Transactions from Address: %s, to Address: %s", fromAddressNode, toAddressNode);
        if (fromAddressNode == null || toAddressNode == null) {
            return 0.0;
        }

        Iterable<Relationship> transactions = fromAddressNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("Transactions"));
        double totalAmount = 0.0;
        for (Relationship relationship : transactions) {
            if (relationship.getOtherNode(fromAddressNode).equals(toAddressNode)) {
                Object amount = relationship.getProperty("amount");
                if (amount instanceof Number) {
                    totalAmount += ((Number) amount).doubleValue();
                }
            }
        }
        return totalAmount;
    }
}
