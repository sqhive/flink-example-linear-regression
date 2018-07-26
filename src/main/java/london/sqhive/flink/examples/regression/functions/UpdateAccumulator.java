package london.sqhive.flink.examples.regression.functions;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import london.sqhive.flink.examples.regression.model.Params;

/**
 * Accumulator all the update.
 * */
public class UpdateAccumulator
    implements ReduceFunction<Tuple2<Params, Integer>> {

    @Override
    public Tuple2<Params, Integer> reduce(Tuple2<Params, Integer> val1, Tuple2<Params, Integer> val2) {

        double newTheta0 = val1.f0.getTheta0() + val2.f0.getTheta0();
        double newTheta1 = val1.f0.getTheta1() + val2.f0.getTheta1();
        Params result = new Params(newTheta0, newTheta1);
        return new Tuple2<Params, Integer>(result, val1.f1 + val2.f1);

    }
}
