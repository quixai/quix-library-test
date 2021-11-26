from typing import List


from quixstreaming import *
import signal
import threading


# Create a client. Client helps you to create input reader or output writer for specified topic.
security = SecurityOptions('{placeholder:broker.security.certificatepath}', "{placeholder:broker.security.username}", "{placeholder:broker.security.password}")
client = StreamingClient('{placeholder:broker.address}', security)


input_topic = client.open_input_topic('{placeholder:inputTopic}')
# read streams
def read_stream(new_stream: StreamReader):
    print("New Stream read!")

    def on_stream_closed_handler(end_type: StreamEndType):
        print("Stream", new_stream.stream_id, "closed with", end_type)

    new_stream.on_stream_closed += on_stream_closed_handler

    def on_stream_properties_changed_handler():
        properties = new_stream.properties
        print("Stream properties read for stream: " + new_stream.stream_id)
        print("Name", properties.name, sep=": ")
        print("Location", properties.location, sep=": ")
        print("Metadata", properties.metadata, sep=": ")
        # print(properties.metadata["meta"]) # or by index
        print("Parents", properties.parents, sep=": ")
        # print(properties.parents[0]) # or by index
        print("TimeOfRecording", properties.time_of_recording, sep=": ")

    new_stream.properties.on_changed += on_stream_properties_changed_handler

    def on_parameter_data_handler(data: ParameterData):
        print("Parameter data read for stream: " + new_stream.stream_id)
        print("  Length:", len(data.timestamps))
        for index, val in enumerate(data.timestamps):
            print("    Time:", val)
            tagstring = "    Tags: "
            for tag, vals in data.timestamps[index].tags.items():
                tagstring = tagstring + tag + "=" + str(vals[index]) + ", "
            tagstring.rstrip(", ")
            print(tagstring)
            for timestamps in data.timestamps:
                for key, value in timestamps.parameters.items():
                    print("      " + key + ": " + str(value.numeric_value))
                    print("      " + key + ": " + str(value.string_value))

    new_stream.parameters.create_buffer().on_read += on_parameter_data_handler

    def on_parameter_definitions_changed_handler():
        print("Parameter definitions read for stream: " + new_stream.stream_id)
        indent = "   "

        def print_parameters(params: List[ParameterDefinition], level):
            print(level * indent + "Parameters:")
            for parameter in params:
                print((level + 1) * indent + parameter.id + ": ")
                if parameter.name is not None:
                    print((level + 2) * indent + "Name: " + parameter.name)
                if parameter.description is not None:
                    print((level + 2) * indent + "Description: " + parameter.description)
                if parameter.format is not None:
                    print((level + 2) * indent + "Format: " + parameter.format)
                if parameter.unit is not None:
                    print((level + 2) * indent + "Unit: " + parameter.unit)
                if parameter.maximum_value is not None:
                    print((level + 2) * indent + "Maximum value: " + str(parameter.maximum_value))
                if parameter.minimum_value is not None:
                    print((level + 2) * indent + "Minimum value: " + str(parameter.minimum_value))
                if parameter.custom_properties is not None:
                    print((level + 2) * indent + "Custom properties: " + parameter.custom_properties)

        print_parameters(new_stream.parameters.definitions, 0)

    new_stream.parameters.on_definitions_changed += on_parameter_definitions_changed_handler

    def on_event_definitions_changed_handler():
        print("Event definitions read for stream: " + new_stream.stream_id)
        indent = "   "

        def print_events(params: List[EventDefinition], level):
            print(level * indent + "Events:")
            for event in params:
                print((level + 1) * indent + event.id + ": ")
                if event.name is not None:
                    print((level + 2) * indent + "Name: " + event.name)
                print((level + 2) * indent + "Level: " + str(event.level))
                if event.description is not None:
                    print((level + 2) * indent + "Description: " + event.description)
                if event.custom_properties is not None:
                    print((level + 2) * indent + "Custom properties: " + event.custom_properties)

        print_events(new_stream.events.definitions, 0)

    new_stream.events.on_definitions_changed += on_event_definitions_changed_handler

    def on_event_data_handler(data: EventData):
        print("Event data read for stream: " + new_stream.stream_id)
        print("  Time:", data.timestamp)
        print("  Id:", data.id)
        tagstring = "  Tags: "
        for tag, val in data.tags.items():
            tagstring = tagstring + tag + "=" + str(val) + ", "
        tagstring.rstrip(", ")
        print(tagstring)
        print("  Value: " + data.value)

    new_stream.events.on_read += on_event_data_handler


input_topic.on_stream_received += read_stream
input_topic.start_reading()

# Hook up to termination signal (for docker image) and CTRL-C
print("Listening to streams. Press CTRL-C to exit.")


event = threading.Event() 
def signal_handler(sig, frame):
    print('Exiting...')
    event.set()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
event.wait()