"""Build a protobuf message class from a Schema Registry `.proto` schema.

Confluent's ProtobufDeserializer needs a Python message class, but the schema
itself lives in Schema Registry exactly as it does for Avro and JSON Schema.
Rather than making callers pre-generate and mount `_pb2.py` files, the
registered schema is compiled in-process and the class built from it.

This needs packages the rest of the tool does not (`grpcio-tools` pulls in
`grpcio`, roughly 19 MiB), so they are an optional install and every import of
them is deferred to the point of use. See requirements-protobuf.txt.
"""

import logging
import os
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

MISSING_DEPENDENCY_HINT = (
    "VALUE_DESERIALIZER/KEY_DESERIALIZER is set to ProtobufDeserializer, which needs "
    "packages that are not installed by default. Install them with: "
    "pip install -r requirements-protobuf.txt"
)


class ProtobufSchemaError(RuntimeError):
    """A registered .proto schema could not be turned into a message class."""


def _imports():
    """Import the optional protobuf toolchain, or explain how to get it."""
    try:
        import grpc_tools
        from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
        from grpc_tools import protoc
    except ImportError as e:
        raise ProtobufSchemaError(f"{MISSING_DEPENDENCY_HINT} (missing: {e.name})") from e
    return grpc_tools, protoc, descriptor_pb2, descriptor_pool, message_factory


def _well_known_include(grpc_tools) -> str:
    """Where grpc_tools keeps google/protobuf/*.proto, so imports of the
    well-known types (Timestamp, Duration, ...) resolve without extra setup."""
    return os.path.join(os.path.dirname(grpc_tools.__file__), '_proto')


def _write_schema_tree(root: Path, filename: str, schema, schema_registry_client) -> None:
    """Write a schema and, recursively, every schema it imports.

    A registered .proto may `import "common/header.proto"`, recorded as a
    reference naming another subject. protoc needs those files on disk under
    the import path the .proto uses.
    """
    target = root / filename
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(schema.schema_str, encoding='utf-8')

    for reference in getattr(schema, 'references', None) or []:
        if (root / reference.name).exists():
            continue
        if schema_registry_client is None:
            raise ProtobufSchemaError(
                f"Schema imports {reference.name!r}, which needs a Schema Registry client to resolve"
            )
        registered = schema_registry_client.get_version(reference.subject, reference.version)
        _write_schema_tree(root, reference.name, registered.schema, schema_registry_client)


ENTRY_FILENAME = 'schema.proto'


def _descriptor_set(schema, schema_registry_client, protoc, grpc_tools) -> bytes:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        entry = ENTRY_FILENAME
        _write_schema_tree(root, entry, schema, schema_registry_client)

        output = root / 'schema.desc'
        result = protoc.main([
            'protoc',
            f'-I{root}',
            f'-I{_well_known_include(grpc_tools)}',
            f'--descriptor_set_out={output}',
            '--include_imports',
            str(root / entry),
        ])
        if result != 0 or not output.exists():
            raise ProtobufSchemaError(
                f"protoc failed (exit {result}) on the registered schema; see the log above for details"
            )
        return output.read_bytes()


def build_message_class(schema, schema_registry_client=None, message_name: str = None):
    """Compile a registered .proto schema and return a message class.

    `message_name` selects a specific fully-qualified message. Without it the
    first message declared in the schema is used, which is what Confluent's
    default message index (0) refers to.
    """
    grpc_tools, protoc, descriptor_pb2, descriptor_pool, message_factory = _imports()

    file_descriptor_set = descriptor_pb2.FileDescriptorSet()
    file_descriptor_set.ParseFromString(
        _descriptor_set(schema, schema_registry_client, protoc, grpc_tools)
    )

    pool = descriptor_pool.DescriptorPool()
    for file_proto in file_descriptor_set.file:
        pool.Add(file_proto)

    available = _message_names(file_descriptor_set)
    if not available:
        raise ProtobufSchemaError("The registered schema declares no message types")

    if message_name is None:
        message_name = available[0]
        if len(available) > 1:
            logger.info(
                f"Schema declares {len(available)} message types; using {message_name!r}. "
                "Set PROTOBUF_MESSAGE_NAME to choose a different one."
            )
    elif message_name not in available:
        raise ProtobufSchemaError(
            f"Message {message_name!r} is not in the registered schema; available: {', '.join(available)}"
        )

    return message_factory.GetMessageClass(pool.FindMessageTypeByName(message_name))


def _message_names(file_descriptor_set) -> list:
    """Fully-qualified top-level message names, the registered schema's first.

    protoc --include_imports puts dependencies *before* the file that imports
    them, so the registered schema's own messages have to be pulled to the
    front. Otherwise a schema that imports another subject would default to a
    message from the import rather than its own.
    """
    own, imported = [], []
    for file_proto in file_descriptor_set.file:
        if file_proto.name.startswith('google/protobuf/'):
            continue
        prefix = f'{file_proto.package}.' if file_proto.package else ''
        names = [f'{prefix}{message.name}' for message in file_proto.message_type]
        (own if file_proto.name == ENTRY_FILENAME else imported).extend(names)
    return own + imported


def to_dict(message):
    """Convert a decoded protobuf message to plain dicts.

    The rest of the tool reads timestamps out of mappings, so the message is
    converted rather than special-cased. Field names are preserved as declared
    in the .proto, so T1=value.produced_at means what it says; note that
    protobuf's JSON mapping renders 64-bit integers as strings, which the
    epoch conversion already accepts.
    """
    from google.protobuf.json_format import MessageToDict

    return MessageToDict(message, preserving_proto_field_name=True)


def build_protobuf_deserializer(schema, schema_registry_client=None, message_name: str = None):
    """A callable that decodes Confluent-framed protobuf into plain dicts.

    Everything protobuf-specific is imported here rather than at module scope,
    so the tool runs normally when the optional packages are absent and only
    complains if a protobuf deserializer is actually configured.
    """
    try:
        from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
    except ImportError as e:
        raise ProtobufSchemaError(f"{MISSING_DEPENDENCY_HINT} (missing: {e.name})") from e

    message_class = build_message_class(
        schema, schema_registry_client=schema_registry_client, message_name=message_name
    )
    deserializer = ProtobufDeserializer(message_class, {'use.deprecated.format': False})

    def deserialize(raw, context):
        decoded = deserializer(raw, context)
        return None if decoded is None else to_dict(decoded)

    return deserialize
