import lsst.alert.packet as lap

from lass.lassproducer import (
    _alert_serializer,
    generate,
)

def test_avro_serializer_round_trip():
    alert = generate(numalerts=1)[0][0]
    payload = _alert_serializer(avro=True)(alert)
    schema_path = lap.get_schema_path(10, 0) + "/lsst.v10_0.alert.avsc"
    decoded = lap.Schema.from_file(filename=schema_path).deserialize(payload)

    assert decoded["diaSourceId"] == alert["diaSourceId"]
