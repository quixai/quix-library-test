from quixstreaming import *
from quixstreaming.models.parametersbufferconfiguration import ParametersBufferConfiguration
import sys
import signal
import threading


# Create a client. Client helps you to create input reader or output writer for specified topic.
security = SecurityOptions('{placeholder:broker.security.certificatepath}', "{placeholder:broker.security.username}", "{placeholder:broker.security.password}")
client = StreamingClient('{placeholder:broker.address}', security)


input_topic = client.open_input_topic('{placeholder:inputTopic}')


# read streams
def read_stream(new_stream: StreamReader):
    print("New stream read:" + new_stream.stream_id)

    buffer_options = ParametersBufferConfiguration()
    buffer_options.time_span_in_milliseconds = 100

    buffer = new_stream.parameters.create_buffer(buffer_options)

    def on_parameter_data_handler(data: ParameterData):

        df = data.to_panda_frame()
        print(df.to_string())

    buffer.on_read += on_parameter_data_handler


# Hook up events before initiating read to avoid losing out on any data
input_topic.on_stream_received += read_stream
input_topic.start_reading()  # initiate read

# Hook up to termination signal (for docker image) and CTRL-C
print("Listening to streams. Press CTRL-C to exit.")


event = threading.Event() 
def signal_handler(sig, frame):
    print('Exiting...')
    event.set()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
event.wait()