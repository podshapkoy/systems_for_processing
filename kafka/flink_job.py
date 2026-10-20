import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

KAFKA_BROKER = '192.168.34.2:9092'
INPUT_TOPIC = 'input_topic'


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///app/flink-sql-connector-kafka-1.17.2.jar")
    env.set_parallelism(1)

    type_info = Types.ROW_NAMED(
        ['device_id', 'temperature', 'execution_time'],
        [Types.INT(), Types.INT(), Types.INT()]
    )
    deserialization_schema = JsonRowDeserializationSchema.builder().type_info(type_info).build()

    kafka_props = {'bootstrap.servers': KAFKA_BROKER, 'group.id': 'hw_flink_group'}
    consumer = FlinkKafkaConsumer(INPUT_TOPIC, deserialization_schema, kafka_props)

    consumer.set_start_from_earliest()

    stream = env.add_source(consumer)

    def create_string_sink(topic_name):
        return FlinkKafkaProducer(
            topic_name,
            SimpleStringSchema(),
            kafka_props
        )

    # Задание 1
    def task1_map(row):
        return json.dumps({
            "device_id": row.device_id,
            "temperature": row.temperature,
            "execution_time": row.execution_time
        })

    stream.map(task1_map, output_type=Types.STRING()).add_sink(create_string_sink('topic_task_1'))

    # Задание 2
    def task2_map(row):
        f_temp = row.temperature * 1.8 + 32
        return json.dumps({
            "device_id": row.device_id,
            "temperature": f_temp,
            "execution_time": row.execution_time
        })

    stream.map(task2_map, output_type=Types.STRING()).add_sink(create_string_sink('topic_task_2'))

    stream.filter(lambda row: row.temperature > 50) \
        .map(task1_map, output_type=Types.STRING()) \
        .add_sink(create_string_sink('topic_task_3'))

    class GlobalAggProcess(KeyedProcessFunction):
        def open(self, runtime_context: RuntimeContext):
            state_desc = ValueStateDescriptor('global_sum_count', Types.STRING())
            self.state = runtime_context.get_state(state_desc)

        def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
            state_val = self.state.value()
            if state_val is None:
                current_sum, current_count = 0, 0
            else:
                current_sum, current_count = map(float, state_val.split(","))

            new_sum = current_sum + value.temperature
            new_count = current_count + 1

            self.state.update(f"{new_sum},{new_count}")

            avg_temp = new_sum / new_count

            yield json.dumps({
                "device_id": value.device_id,
                "temperature": value.temperature,
                "execution_time": value.execution_time,
                "average_temperature": float(avg_temp)
            })

    stream.key_by(lambda row: 1, key_type=Types.INT()) \
        .process(GlobalAggProcess(), output_type=Types.STRING()) \
        .add_sink(create_string_sink('topic_task_4'))

    class Last5Process(KeyedProcessFunction):
        def open(self, runtime_context: RuntimeContext):
            state_desc = ValueStateDescriptor('last_5_temps', Types.STRING())
            self.state = runtime_context.get_state(state_desc)

        def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
            state_val = self.state.value()
            if state_val is None:
                history = []
            else:
                history = json.loads(state_val)

            history.append(value.temperature)

            if len(history) > 5:
                history.pop(0)

            self.state.update(json.dumps(history))

            avg_temp = sum(history) / len(history)

            yield json.dumps({
                "device_id": value.device_id,
                "temperature": value.temperature,
                "execution_time": value.execution_time,
                "average_temperature": float(avg_temp)
            })

    stream.key_by(lambda row: row.device_id, key_type=Types.INT()) \
        .process(Last5Process(), output_type=Types.STRING()) \
        .add_sink(create_string_sink('topic_task_5'))

    env.execute("IoT_Temperature_Processing_Tasks")


if __name__ == '__main__':
    main()
