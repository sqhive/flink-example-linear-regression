package london.sqhive.flink.examples.regression.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import london.sqhive.flink.examples.regression.model.Data;
import london.sqhive.flink.examples.regression.model.Params;

import java.util.Collection;

/**
 * Compute a single BGD type update for every parameters.
 */
public class SubUpdate
    extends RichMapFunction<Data, Tuple2<Params, Integer>> {

    private Collection<Params> parameters;

    private Params parameter;

    private int count = 1;

    /** Reads the parameters from a broadcast variable into a collection. */
    @Override
    public void open(Configuration parameters) throws Exception {
        this.parameters = getRuntimeContext().getBroadcastVariable("parameters");
    }

    @Override
    public Tuple2<Params, Integer> map(Data in) throws Exception {

        for (Params p : parameters){
            this.parameter = p;
        }

        double theta0 = parameter.getTheta0() - 0.01 * ((parameter.getTheta0() + (parameter.getTheta1() * in.x)) - in.y);
        double theta1 = parameter.getTheta1() - 0.01 * (((parameter.getTheta0() + (parameter.getTheta1() * in.x)) - in.y) * in.x);

        return new Tuple2<Params, Integer>(new Params(theta0, theta1), count);
    }
}