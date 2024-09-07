import pytest
from lib.utils import get_spark_session


# setup should be part of fixture and not the test case
# yield 
@pytest.fixture
def spark():
    """Creates spark session"""
    spark_session = get_spark_session("LOCAL")
    yield spark_session
    spark_session.stop()

@pytest.fixture
def expected_results(spark):
    """Gives Expected Results"""
    results_schema = "state string, count integer"
    return spark.read \
            .format("csv") \
            .schema(results_schema) \
            .load("data/test_results/state_aggregate.csv")
