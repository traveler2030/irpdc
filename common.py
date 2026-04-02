"""
공통 유틸리티: 설정 로드, 로깅, HTTP 재시도
"""
import json
import logging
import time
from pathlib import Path

BASE_DIR = Path(__file__).parent
CONFIG_FILE = BASE_DIR / "config.json"

_config_cache = None


def load_config():
    global _config_cache
    if _config_cache is not None:
        return _config_cache
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            _config_cache = json.load(f)
    else:
        _config_cache = {}
    return _config_cache


def get_reference_date():
    """기준일자 반환. config에 없으면 오늘 날짜."""
    cfg = load_config()
    d = cfg.get("reference_date", "")
    if d:
        return d
    from datetime import datetime
    return datetime.now().strftime("%Y%m%d")


def setup_logging(name, level=logging.INFO):
    """로거 설정 (콘솔 + 파일)"""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        logger.addHandler(ch)
        fh = logging.FileHandler(BASE_DIR / f"{name}.log", encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger


def retry_request(session, method, url, max_attempts=3, backoff_base=2, **kwargs):
    """HTTP 요청 재시도. 성공 시 Response, 실패 시 None."""
    cfg = load_config().get("retry", {})
    max_attempts = cfg.get("max_attempts", max_attempts)
    backoff_base = cfg.get("backoff_base", backoff_base)

    for attempt in range(1, max_attempts + 1):
        try:
            resp = getattr(session, method)(url, **kwargs)
            if resp.status_code == 429:
                wait = backoff_base ** attempt
                logging.getLogger(__name__).warning("429 Too Many Requests, %ds 대기...", wait)
                time.sleep(wait)
                continue
            return resp
        except Exception as e:
            if attempt < max_attempts:
                wait = backoff_base ** (attempt - 1)
                logging.getLogger(__name__).warning(
                    "요청 실패 (시도 %d/%d): %s — %ds 후 재시도",
                    attempt, max_attempts, str(e)[:80], wait
                )
                time.sleep(wait)
            else:
                logging.getLogger(__name__).error(
                    "요청 최종 실패: %s — %s", url[:80], str(e)[:80]
                )
    return None
