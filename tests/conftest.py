import pytest

from pyspark.sql import SparkSession

@pytest.fixture(scope="module")
def spark():
    """A fixture to create a Spark Context to reuse across tests."""
    s = SparkSession.builder.appName('pytest-local-spark').master('local') \
        .getOrCreate()

    yield s

    s.stop()
    
# imprimir resultado de cada prueba
def pytest_runtest_logreport(report):
    if report.when == 'call':
        if report.passed:
            status = "✅ PASO"
        elif report.failed:
            status = "❌ FALLÓ"
        elif report.skipped:
            status = "⚠️  OMITIDA"
        else:
            status = "❓ DESCONOCIDO"
        print(f"{status}: {report.nodeid}")
