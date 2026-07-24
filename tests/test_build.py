import lass

# smoke test for pypi package, run by github workflow
if __name__ == '__main__':
    alerts, alertsizes = lass.generate(numalerts=10)
    assert len(alerts) == 10
