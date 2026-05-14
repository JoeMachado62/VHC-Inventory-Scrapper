from __future__ import annotations

import time
from typing import Any

import httpx

from ove_scraper.schemas import DetailPayload, IngestPayload, PendingDetailRequest


class ApiClientError(RuntimeError):
    """Raised when the VCH API cannot be used safely."""


class VCHApiClient:
    def __init__(
        self,
        base_url: str,
        service_token: str,
        *,
        timeout: float = 120.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.root_url = self.base_url.rsplit("/v1", 1)[0] if "/v1" in self.base_url else self.base_url
        self.headers = {
            "X-Service-Token": service_token,
            "Content-Type": "application/json",
        }
        self.client = httpx.Client(headers=self.headers, timeout=timeout, transport=transport)

    def close(self) -> None:
        self.client.close()

    def check_health(self) -> bool:
        try:
            response = self.client.get(f"{self.root_url}/health")
            return response.status_code == 200
        except httpx.HTTPError:
            return False

    def push_ove_ingest(self, vehicles: list[dict[str, Any]], sync_metadata: dict[str, Any]) -> dict[str, Any]:
        payload = IngestPayload.model_validate({"vehicles": vehicles, "sync_metadata": sync_metadata})
        response = self._request_with_retry(
            "POST",
            f"{self.base_url}/inventory/ove/ingest",
            json=payload.model_dump(mode="json"),
        )
        return response.json()

    def push_ove_detail(self, vin: str, detail_payload: dict[str, Any]) -> dict[str, Any]:
        payload = DetailPayload.model_validate(detail_payload)
        response = self._request_with_retry(
            "POST",
            f"{self.base_url}/inventory/ove/detail/{vin}",
            json=payload.model_dump(mode="json"),
        )
        return response.json()

    def push_hot_deals_batch(
        self,
        batch_payload: dict[str, Any],
        *,
        endpoint_path: str = "/inventory/ove/hot-deals/ingest",
    ) -> dict[str, Any]:
        """POST the curated Hot Deals batch to the VPS.

        Per HOT_DEALS_SCRAPER_CONTRACT.md: this is a single-batch push
        per completed VHC Marketing List run. Use snapshot_mode=
        "full_replace" so the VPS can deactivate hot deals that fell
        off the latest list.

        Note: contract specifies an Authorization: Bearer header for
        this endpoint, distinct from the X-Service-Token header the
        rest of the API uses. We add Bearer for this one POST without
        mutating the client's default headers.
        """
        # Caller passes a fully-built batch dict (no Pydantic schema —
        # the VPS contract is loose enough that strict validation here
        # would block legitimate edge fields). The VPS does its own
        # per-VIN validation and returns a per-VIN error list when
        # entries are rejected.
        bearer_headers = dict(self.headers)
        bearer_headers["Authorization"] = bearer_headers.get(
            "Authorization", f"Bearer {self.headers.get('X-Service-Token', '')}"
        )
        response = self._request_with_retry(
            "POST",
            f"{self.base_url}{endpoint_path}",
            json=batch_payload,
            headers=bearer_headers,
        )
        return response.json()

    def claim_pending_detail_requests(
        self,
        *,
        worker_id: str,
        limit: int = 1,
        lease_seconds: int = 900,
    ) -> list[PendingDetailRequest]:
        response = self._request_with_retry(
            "POST",
            f"{self.base_url}/inventory/ove/detail/claim",
            json={
                "worker_id": worker_id,
                "limit": limit,
                "lease_seconds": lease_seconds,
            },
        )
        body = response.json()
        items = body.get("data", {}).get("items", [])
        return [PendingDetailRequest.model_validate(item) for item in items]

    def complete_detail_request(
        self,
        request_id: str,
        *,
        worker_id: str,
        result: str = "success",
    ) -> dict[str, Any]:
        response = self._request_with_retry(
            "POST",
            f"{self.base_url}/inventory/ove/detail/{request_id}/complete",
            json={
                "worker_id": worker_id,
                "result": result,
            },
        )
        return response.json()

    def fail_detail_request(
        self,
        request_id: str,
        *,
        worker_id: str,
        error_category: str,
        error_message: str,
        retry_after_seconds: int,
    ) -> dict[str, Any]:
        response = self._request_with_retry(
            "POST",
            f"{self.base_url}/inventory/ove/detail/{request_id}/fail",
            json={
                "worker_id": worker_id,
                "error_category": error_category,
                "error_message": error_message,
                "retry_after_seconds": retry_after_seconds,
            },
        )
        return response.json()

    def terminal_detail_request(
        self,
        request_id: str,
        *,
        worker_id: str,
        reason: str,
        message: str,
    ) -> dict[str, Any]:
        response = self._request_with_retry(
            "POST",
            f"{self.base_url}/inventory/ove/detail/{request_id}/terminal",
            json={
                "worker_id": worker_id,
                "reason": reason,
                "message": message,
            },
        )
        return response.json()

    def heartbeat_detail_request(
        self,
        request_id: str,
        *,
        worker_id: str,
        lease_seconds: int,
    ) -> dict[str, Any]:
        response = self._request_with_retry(
            "POST",
            f"{self.base_url}/inventory/ove/detail/{request_id}/heartbeat",
            json={
                "worker_id": worker_id,
                "lease_seconds": lease_seconds,
            },
        )
        return response.json()

    def send_scraper_heartbeat(
        self,
        *,
        worker_id: str,
        profile: str | None = None,
        scraper_version: str | None = None,
        node_id: str | None = None,
        last_sync_at: str | None = None,
        last_poll_at: str | None = None,
        last_claim_at: str | None = None,
        pending_claims: int | None = None,
        status_note: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Best-effort liveness heartbeat to the VPS.

        Per the 2026-04-08 VPS handoff: partial heartbeats are accepted —
        only fields present in the body overwrite server-side state, null
        / missing fields preserve the prior value. The VPS warning
        threshold is 5 minutes, critical is 15 minutes; the scraper main
        loop sends one heartbeat per ~30s polling tick to stay well under.

        This method NEVER raises. Heartbeats are best-effort by contract:
        a transient network failure on the heartbeat path must not stop
        the scraper from doing real work. Returns None on any failure.
        """
        body: dict[str, Any] = {"worker_id": worker_id}
        if profile is not None:
            body["profile"] = profile
        if scraper_version is not None:
            body["scraper_version"] = scraper_version
        if node_id is not None:
            body["node_id"] = node_id
        if last_sync_at is not None:
            body["last_sync_at"] = last_sync_at
        if last_poll_at is not None:
            body["last_poll_at"] = last_poll_at
        if last_claim_at is not None:
            body["last_claim_at"] = last_claim_at
        if pending_claims is not None:
            body["pending_claims"] = pending_claims
        if status_note is not None:
            body["status_note"] = status_note
        if details is not None:
            body["details"] = details
        try:
            # Heartbeats are tight-budget calls; do NOT use the long
            # retry-with-backoff path. A single try with a short timeout
            # keeps the polling loop responsive.
            response = self.client.post(
                f"{self.base_url}/inventory/ove/scraper-heartbeat",
                json=body,
                timeout=10.0,
            )
            if response.status_code >= 400:
                return None
            try:
                return response.json()
            except Exception:
                return None
        except Exception:
            return None

    def _request_with_retry(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        backoffs = [0, 2, 4, 8]
        last_error: Exception | None = None

        for attempt, delay in enumerate(backoffs, start=1):
            if delay:
                time.sleep(delay)
            try:
                response = self.client.request(method, url, **kwargs)
                if response.status_code == 401:
                    raise ApiClientError("VCH API authentication failed (401)")
                if response.status_code >= 500:
                    raise ApiClientError(
                        "VCH API server error "
                        f"{response.status_code} for {url}: {response.text[:2000]}"
                    )
                if response.status_code >= 400:
                    raise ApiClientError(f"VCH API rejected request with status {response.status_code}: {response.text}")
                return response
            except ApiClientError:
                raise
            except (httpx.TimeoutException, httpx.NetworkError, httpx.HTTPStatusError) as exc:
                last_error = exc
                if attempt == len(backoffs):
                    break

        raise ApiClientError(f"VCH API request failed after retries: {last_error}")
