import lass

alerts, alertsizes = lass.generate()
lass.startbroker()
lass.send(alerts)
lass.plotAlertSizeDistibution(alertsizes)
lass.pprint_alert(alerts[0], save=False)
lass.removebroker()
