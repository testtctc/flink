package org.apache.flink.streaming.api;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.lang.reflect.Field;

public class SocketWindowWordCount {

	public static void main(String[] args) throws Exception {

		// 创建 execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//强行获取
		Field field = Class.forName("org.apache.flink.streaming.api.environment.StreamExecutionEnvironment").getDeclaredField("configuration");
		field.setAccessible(true);
		Configuration configuration = (Configuration) field.get(env);
		configuration.setString("rest.bind-port", "8087");

		ExecutionConfig con =  env.getConfig();
		con.setMaxParallelism(100);
		//为了方便调试
		con.setParallelism(1);
		DataStreamSource<String> source= env.fromElements("hello","nice","hello");
		source.filter(x->x.length()>2).name("filter-2").uid("filter-2").keyBy(x->x).process(
			new KeyedProcessFunction<String, String, Tuple2<String,Integer>>(){
				public void processElement(String value,KeyedProcessFunction<String, String, Tuple2<String,Integer>>.Context ctx,
				Collector<Tuple2<String,Integer>> out) throws Exception{
					out.collect(Tuple2.of(value,1));
				}
			}
		).name("process-3").uid("process-3").print();

		env.execute("Socket Window WordCount");
	}
}
