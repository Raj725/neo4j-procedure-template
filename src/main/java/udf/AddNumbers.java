package udf;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.List;

/**
 * @author RajendraK
 */

public class AddNumbers {

    @UserFunction
    @Description("udf.addNumbers([1,2,4,...]) - add the given numbers.")
    public Long addNumbers(@Name("numbers") List<Long> numbers) {
        if (numbers == null) {
            return null;
        }
        return numbers.stream().mapToLong(Long::longValue).sum();
    }

}
