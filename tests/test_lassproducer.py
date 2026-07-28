import lsst.alert.packet as lap

from lass.lassproducer import (
    _alert_serializer,
    _prefer_non_null_dia_object,
    default_fastdb_writer,
    generate,
)


def test_generate_populates_dia_object():
    alerts, _ = generate(numalerts=1)

    assert alerts[0]["diaObject"] is not None
    assert alerts[0]["diaObject"]["diaObjectId"] == alerts[0]["diaSource"]["diaObjectId"]


def test_prefer_non_null_dia_object_reorders_union():
    schema_path = lap.get_schema_path(10, 0) + "/lsst.v10_0.alert.avsc"
    schema = lap.Schema.from_file(filename=schema_path).definition

    _prefer_non_null_dia_object(schema)

    dia_object = next(field for field in schema["fields"] if field["name"] == "diaObject")
    assert dia_object["type"][0] != "null"


def test_default_fastdb_writer_matches_ids():
    alert = {"diaObject": {}, "diaSource": {"band": None}}

    result = default_fastdb_writer(alert)

    assert result["diaObject"]["diaObjectId"] == result["diaSource"]["diaObjectId"]
    assert result["diaSourceId"] == result["diaSource"]["diaSourceId"]


def test_avro_serializer_round_trip():
    alert = generate(numalerts=1, alertwriter=lambda record: record)[0][0]
    payload = _alert_serializer(avro=True)(alert)
    schema_path = lap.get_schema_path(10, 0) + "/lsst.v10_0.alert.avsc"
    decoded = lap.Schema.from_file(filename=schema_path).deserialize(payload)

    assert decoded["diaSourceId"] == alert["diaSourceId"]
