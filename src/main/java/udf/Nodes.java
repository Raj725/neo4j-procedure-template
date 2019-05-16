package udf;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author RajendraK
 */

public class Nodes {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @UserFunction
    @Description("udf.getNodeByProperty('s1','s2') find and return the given company node")
    public Node getNodeByProperty(@Name("key") String key, @Name("value") String value) {
        log.info("Key: %s, Value: %s", key, value);
        if (key == null || value == null) {
            log.info("Null");
            return null;
        }
        return db.findNode(Label.label("Company"), key, value);
    }

    @UserFunction
    @Description("udf.getNodeByLabelProperty('s1','s2','s3') find and return the given nodes")
    public List<Node> getNodeByLabelProperty(@Name("label") String label, @Name("key") String key, @Name("value") String value) {
        log.info("Label: %s, Key: %s, Value: %s", label, key, value);
        if (label == null || key == null || value == null) {
            log.info("Null");
            return null;
        }
        return db.findNodes(Label.label(label), key, value)
                 .stream()
                 .collect(Collectors.toList());
    }

}
