package udf;

import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author RajendraK
 */
public class Transactions {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @UserFunction
    @Description("udf.getAddressNode('address') find and return the addresses")
    public Node getAddressNode(@Name("value") String address) {
        log.info("Getting Transactions for Address: %s", address);
        if (address == null) {
            log.info("Null Address");
            return null;
        }
        return db.findNode(Label.label("Addresses"), "blockchain_address", address);
    }

    @UserFunction
    @Description("udf.getTransactionAddressesForAddress('address') find and return the addresses this address has transacted")
    public List<Relationship> getTransactionAddressesForAddress(@Name("value") String address) {
        log.info("Getting Transactions for Address: %s", address);
        if (address == null) {
            log.info("Null Address");
            return null;
        }
        Node addressNode = db.findNode(Label.label("Addresses"), "blockchain_address", address);
        Iterable<Relationship> transactions = addressNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("Transactions"));
        List<Relationship> rels = new ArrayList<>();
        transactions.forEach(rels::add);
        return rels;
    }

    @UserFunction
    @Description("udf.getTransactionAmountByFromAddresses('address') return the total trancsactions from this address")
    public Double getTransactionAmountByFromAddresses(@Name("value") String address) {
        log.info("Getting Transactions for Address: %s", address);
        if (address == null) {
            log.info("Null Address");
            return 0.0;
        }
        Node addressNode = db.findNode(Label.label("Addresses"), "blockchain_address", address);
        Iterable<Relationship> transactions = addressNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("Transactions"));
        double totalAmount = 0.0;
        for (Relationship relationship : transactions) {
            Object amount = relationship.getProperty("amount");
            if (amount instanceof Number) {
                totalAmount += ((Number) amount).doubleValue();
            }
        }
        return totalAmount;
    }

    @UserFunction
    @Description("udf.getTransactionAmountByFromAddressesToAddress('fromAddress','toAddress') return the total trancsactions from this address")
    public Double getTransactionAmountByFromAddressesToAddress(@Name("value") String fromAddress, @Name("value") String toAddress) {
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
}
