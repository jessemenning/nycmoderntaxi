import os

""" Run this file then run how_to_publish_message.py so that it can listen to incoming messages """
from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind
from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.topic import Topic

def direct_message_publish(messaging_service: MessagingService, topic, message):
    try:
        # Create a direct message publish service and start it
        direct_publish_service = messaging_service.create_direct_message_publisher_builder().build()
        direct_publish_service.start_async()
        # Publish the message!
        direct_publish_service.publish(destination=topic, message=message)
    finally:
        direct_publish_service.terminate()

outboundTopic = "opentelemetry/helloworld"

broker_props = {
    "solace.messaging.transport.host": os.environ.get('SOLACE_HOST') or "localhost",
    "solace.messaging.service.vpn-name": os.environ.get('SOLACE_VPN') or "default",
    "solace.messaging.authentication.scheme.basic.username": os.environ.get('SOLACE_USERNAME') or "default",
    "solace.messaging.authentication.scheme.basic.password": os.environ.get('SOLACE_PASSWORD') or "default"
    }

trace.set_tracer_provider(
    TracerProvider(
        resource=Resource.create({SERVICE_NAME: "<Boomi> Listen for Salesforce Platform Account, publish Solace DriverUpserted"})
    )
)

jaeger_exporter = JaegerExporter(
    agent_host_name="localhost",
    agent_port=6831,
)

trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(jaeger_exporter)
)

tracer = trace.get_tracer(__name__)
# THIS IS PER https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/trace/semantic_conventions/messaging.md
parentSpan = tracer.start_span(
    "RideUpdated send",
    kind=SpanKind.PRODUCER,
    attributes={
        "messaging.system": "solace",
        "messaging.destination": outboundTopic,
        "messaging.destination-kind": "topic",
        "messaging.protocol": "jcsmp",
        "messaging.protocol_version": "1.0",
        "messaging.url": os.environ.get('SOLACE_HOST') or "localhost"
        }
)

messaging_service = MessagingService.builder().from_properties(broker_props).build()
messaging_service.connect()

trace_id = parentSpan.get_span_context().trace_id
span_id = parentSpan.get_span_context().span_id


print("parentSpan trace_id  on sender side:" + str(trace_id))
print("parentSpan span_id  on sender side:" + str(span_id))

destination_name = Topic.of(outboundTopic)

outbound_msg = messaging_service.message_builder() \
    .with_property("trace_id", str(trace_id)) \
    .with_property("span_id", str(span_id)) \
    .build("Hello World! This is a message published from Python!")

direct_message_publish(messaging_service, destination_name, outbound_msg)

parentSpan.end()
