import logging
import json
from urllib.request import urlopen
from http import HTTPStatus

from utils import CITIES, ERR_MESSAGE_TEMPLATE
from exceptions import YandexAPIException

logger = logging.getLogger()


class YandexWeatherAPI:
    """
    Base class for requests
    """

    @staticmethod
    def _do_req(url: str) -> dict:
        """Base request method"""
        try:
            with urlopen(url) as req:
                resp = req.read().decode("utf-8")
                resp = json.loads(resp)
            if req.status != HTTPStatus.OK:
                raise YandexAPIException(
                    "Error during execute request. {}: {}".format(
                        resp.status, resp.reason
                    )
                )
            return resp
        except Exception as ex:
            logger.exception(ex)
            raise YandexAPIException(ERR_MESSAGE_TEMPLATE)

    @staticmethod
    def _get_url_by_city_name(city_name: str) -> str:
        try:
            return CITIES[city_name]
        except KeyError:
            raise YandexAPIException(
                "Please check that city {} exists".format(city_name)
            )

    def get_forecasting(self, city_name: str) -> dict:
        """
        :param city_name: key as str
        :return: response data as json
        """
        city_url = self._get_url_by_city_name(city_name)
        return self._do_req(city_url)
