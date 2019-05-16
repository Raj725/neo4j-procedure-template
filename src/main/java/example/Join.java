package example;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.List;

/**
 * This is an example how you can create a simple user-defined function for Neo4j.
 */
public class Join {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @UserFunction
    @Description("example.join(['s1','s2',...], delimiter) - join the given strings with the given delimiter.")
    public String join(
                    @Name("strings") List<String> strings,
                    @Name(value = "delimiter", defaultValue = ",") String delimiter) {
        if (strings == null || delimiter == null) {
            return null;
        }
        return String.join(delimiter, strings);
    }

    @UserFunction
    @Description("example.join(['s1','s2',...], delimiter) - join the given strings with the given delimiter.")
    public Long sumNums(@Name("numbers") List<Long> numbers) {
        if (numbers == null) {
            return null;
        }
        return numbers.stream()
                      .mapToLong(Long::longValue)
                      .sum();
    }

    @UserFunction
    @Description("example.join(['s1','s2',...], delimiter) - join the given strings with the given delimiter.")
    public Node getCompanyByProperty(@Name("key") String key, @Name("value") String value) {
        if (key == null || value == null) {
            return null;
        }
        return db.findNode(Label.label("Company"), key, value);
    }

}