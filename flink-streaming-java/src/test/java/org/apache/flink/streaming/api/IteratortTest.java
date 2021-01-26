package org.apache.flink.streaming.api;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

public class IteratortTest {
	public static void main(String[]args){
		final StreamExecutionEnvironment env =
			StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Long> initialStream = env.fromElements(3L,4L,5L);

		IterativeStream<Long> iteration = initialStream.iterate();
		//迭代操作
		DataStream<Long> iterationBody = iteration.map (x->x-3);
		//反馈流
		DataStream<Long> feedback = iterationBody.filter(new FilterFunction<Long>(){
			@Override
			public boolean filter(Long value) throws Exception {
				return value > 0;
			}
		});
		iteration.closeWith(feedback);

		//结果流
		DataStream<Long> output = iterationBody.filter(new FilterFunction<Long>(){
			@Override
			public boolean filter(Long value) throws Exception {
				return value <= 0;
			}
		});

		output.print();

	}


}
