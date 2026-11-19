"""publisher.py is only a launcher: it must hand over to the API gateway component."""

from kai_sim_cowork import publisher
from kai_sim_cowork.services.api_gateway import main as gateway_main


def test_publisher_launches_the_api_gateway_entry_point():
    assert publisher.main is gateway_main.main
