"""Tests for LocalFilesService stale-while-error fallback."""
import pytest
from unittest.mock import AsyncMock, MagicMock

from core.exceptions import ExternalServiceError


def _make_local_files_service(lidarr=None, cache=None):
    from services.local_files_service import LocalFilesService

    lidarr = lidarr or AsyncMock()
    prefs = MagicMock()
    prefs.get_advanced_settings.return_value = MagicMock(
        cache_ttl_local_files_recently_added=120,
        cache_ttl_local_files_storage_stats=300,
    )
    prefs.get_local_files_connection.return_value = MagicMock(
        music_path="/music", lidarr_root_path="/music"
    )
    cache = cache or AsyncMock()
    return LocalFilesService(
        lidarr_repo=lidarr,
        preferences_service=prefs,
        cache=cache,
    )


class TestStaleWhileError:
    @pytest.mark.asyncio
    async def test_serves_stale_data_when_lidarr_down(self):
        stale_albums = [{"id": 1, "title": "Old Album"}]

        cache = AsyncMock()
        # Primary cache miss, then stale cache hit
        cache.get = AsyncMock(side_effect=lambda key: (
            None if key == "local_files_all_albums" else stale_albums
        ))

        lidarr = AsyncMock()
        lidarr.get_all_albums = AsyncMock(side_effect=ExternalServiceError("Lidarr down"))

        svc = _make_local_files_service(lidarr=lidarr, cache=cache)
        result = await svc._fetch_all_albums()

        assert result == stale_albums

    @pytest.mark.asyncio
    async def test_raises_when_no_stale_data(self):
        cache = AsyncMock()
        cache.get = AsyncMock(return_value=None)  # Both caches miss

        lidarr = AsyncMock()
        lidarr.get_all_albums = AsyncMock(side_effect=ExternalServiceError("Lidarr down"))

        svc = _make_local_files_service(lidarr=lidarr, cache=cache)
        with pytest.raises(ExternalServiceError, match="Lidarr down"):
            await svc._fetch_all_albums()

    @pytest.mark.asyncio
    async def test_successful_fetch_updates_stale_cache(self):
        fresh_albums = [{"id": 2, "title": "Fresh Album"}]

        cache = AsyncMock()
        cache.get = AsyncMock(return_value=None)
        cache.set = AsyncMock()

        lidarr = AsyncMock()
        lidarr.get_all_albums = AsyncMock(return_value=fresh_albums)

        svc = _make_local_files_service(lidarr=lidarr, cache=cache)
        result = await svc._fetch_all_albums()

        assert result == fresh_albums
        # Should have set both primary and stale caches
        assert cache.set.call_count == 2
        calls = {call.args[0] for call in cache.set.call_args_list}
        assert "local_files_all_albums" in calls
        assert "local_files_all_albums:stale" in calls

    @pytest.mark.asyncio
    async def test_cache_hit_returns_without_lidarr_call(self):
        cached = [{"id": 3, "title": "Cached"}]

        cache = AsyncMock()
        cache.get = AsyncMock(return_value=cached)

        lidarr = AsyncMock()
        lidarr.get_all_albums = AsyncMock()

        svc = _make_local_files_service(lidarr=lidarr, cache=cache)
        result = await svc._fetch_all_albums()

        assert result == cached
        lidarr.get_all_albums.assert_not_called()
