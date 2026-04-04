"""Tests that LidarrBase reads URL and API key dynamically from Settings."""
import pytest
from unittest.mock import MagicMock


@pytest.fixture
def mutable_settings():
    settings = MagicMock()
    settings.lidarr_url = "http://old-host:8686"
    settings.lidarr_api_key = "old-key"
    return settings


class TestLidarrDynamicUrl:
    def test_base_url_reads_from_settings_dynamically(self, mutable_settings):
        from repositories.lidarr.base import LidarrBase

        base = LidarrBase(mutable_settings, MagicMock(), MagicMock())
        assert base._base_url == "http://old-host:8686"

        mutable_settings.lidarr_url = "http://192.168.50.99:8686"
        assert base._base_url == "http://192.168.50.99:8686"

    def test_api_key_reads_from_settings_dynamically(self, mutable_settings):
        from repositories.lidarr.base import LidarrBase

        base = LidarrBase(mutable_settings, MagicMock(), MagicMock())
        headers = base._get_headers()
        assert headers["X-Api-Key"] == "old-key"

        mutable_settings.lidarr_api_key = "new-key"
        headers = base._get_headers()
        assert headers["X-Api-Key"] == "new-key"

    def test_media_cover_url_uses_dynamic_base_url(self, mutable_settings):
        from repositories.lidarr.base import LidarrBase

        base = LidarrBase(mutable_settings, MagicMock(), MagicMock())

        url1 = base._build_api_media_cover_url(1, "poster.jpg", 500)
        assert "http://old-host:8686" in url1

        mutable_settings.lidarr_url = "http://new-host:8686"
        url2 = base._build_api_media_cover_url(1, "poster.jpg", 500)
        assert "http://new-host:8686" in url2
        assert "http://old-host:8686" not in url2

    def test_album_cover_url_uses_dynamic_base_url(self, mutable_settings):
        from repositories.lidarr.base import LidarrBase

        base = LidarrBase(mutable_settings, MagicMock(), MagicMock())

        url1 = base._build_api_media_cover_url_album(1, "cover.jpg", 500)
        assert "http://old-host:8686" in url1

        mutable_settings.lidarr_url = "http://new-host:8686"
        url2 = base._build_api_media_cover_url_album(1, "cover.jpg", 500)
        assert "http://new-host:8686" in url2
