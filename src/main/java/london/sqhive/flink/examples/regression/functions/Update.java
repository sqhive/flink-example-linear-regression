package london.sqhive.flink.examples.regression.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import london.sqhive.flink.examples.regression.model.Params;

/**
 * Compute the final update by average them.
 */
public class Update
    implements MapFunction<Tuple2<Params, Integer>, Params> {

    @Override
    public Params map(Tuple2<Params, Integer> arg0) throws Exception {
        return arg0.f0.div(arg0.f1);
    }
}