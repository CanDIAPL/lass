import lass


def test_minimal():
    # Minimal example
    alerts, alertsizes = lass.generate()
    lass.startbroker()
    lass.send(alerts)
    lass.plotAlertSizeDistibution(alertsizes)
    lass.pprint_alert(alerts[0], save=False)
    lass.removebroker()
    assert len(alerts) == 10000


def smc_radec_writer(alert):
    alert = lass.default_fastdb_writer(alert)
    alert['diaObject']['dec'] = -72.6977
    alert['diaObject']['ra'] = 13.1867
    alert['diaSource']['dec'] = -72.6977
    alert['diaSource']['ra'] = 13.1867
    return alert

alerts, alertsizes = lass.generate(
    alertwriter=smc_radec_writer
)
print(alerts[0]['diaObject']['diaObjectId'], alerts[0]['diaSource']['diaObjectId'])
print(alerts[0]['diaObject']['ra'], alerts[0]['diaObject']['dec']) #  13.1867 -72.6977
