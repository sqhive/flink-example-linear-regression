/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package london.sqhive.flink.examples.regression;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.utils.ParameterTool;
import london.sqhive.flink.examples.regression.data.LinearRegressionData;
import london.sqhive.flink.examples.regression.functions.SubUpdate;
import london.sqhive.flink.examples.regression.functions.Update;
import london.sqhive.flink.examples.regression.functions.UpdateAccumulator;
import london.sqhive.flink.examples.regression.model.Data;
import london.sqhive.flink.examples.regression.model.Params;

public class Application {

	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		final int iterations = params.getInt("iterations", 10);

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		// get input x data from elements
		DataSet<Data> data;
		if (params.has("input")) {
			// read data from CSV file
			data = env.readCsvFile(params.get("input"))
					.fieldDelimiter(" ")
					.includeFields(true, true)
					.pojoType(Data.class);
		} else {
			System.out.println("Executing LinearRegression example with default input data set.");
			System.out.println("Use --input to specify file input.");
			data = LinearRegressionData.getDefaultDataDataSet(env);
		}

		// get the parameters from elements
		DataSet<Params> parameters = LinearRegressionData.getDefaultParamsDataSet(env);

		// set number of bulk iterations for SGD linear Regression
		IterativeDataSet<Params> loop = parameters.iterate(iterations);

		DataSet<Params> newParameters = data
				// compute a single step using every sample
				.map(new SubUpdate()).withBroadcastSet(loop, "parameters")
				// sum up all the steps
				.reduce(new UpdateAccumulator())
				// average the steps and update all parameters
				.map(new Update());

		// feed new parameters back into next iteration
		DataSet<Params> result = loop.closeWith(newParameters);

		// emit result
		if (params.has("output")) {
			result.writeAsText(params.get("output"));
			// execute program
			env.execute("Linear Regression example");
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			result.print();
		}
	}
}
