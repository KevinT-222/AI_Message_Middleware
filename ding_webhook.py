# -*- coding: utf-8 -*-
"""
ding_webhook.py
钉钉自定义机器人封装（2025-09 校对）
- 支持：Text、Markdown、Link、ActionCard(整体跳转/独立跳转)、FeedCard
- 支持 @所有人/@指定用户（userIds / mobiles）
- HMAC-SHA256 加签、URL 编码，带 timestamp
- requests.Session + Retry(429/5xx) + 超时 + 代理
- 统一错误处理：HTTP 非 200 或 errcode != 0 抛出 DingRobotError
- 可选日志 Handler：错误自动推送到群
"""

from __future__ import annotations
import time
import hmac
import base64
import hashlib
import logging
import urllib.parse
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Union

import requests
from requests.adapters import HTTPAdapter, Retry


class DingRobotError(RuntimeError):
    """钉钉机器人调用失败"""
    pass


def _hmac_sha256_b64(secret: str, content: str) -> str:
    hmac_code = hmac.new(secret.encode("utf-8"),
                         content.encode("utf-8"),
                         digestmod=hashlib.sha256).digest()
    return base64.b64encode(hmac_code).decode("utf-8")


@dataclass
class DingRobot:
    access_token: str
    secret: str
    timeout: float = 5.0
    proxies: Optional[Dict[str, str]] = None
    extra_query: Optional[Dict[str, str]] = None
    session: requests.Session = field(init=False, repr=False)

    def __post_init__(self):
        self.session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST", "GET"],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        if self.proxies:
            self.session.proxies.update(self.proxies)

    # ========= 基础工具 =========
    def _signed_url(self, now_ms: Optional[int] = None) -> str:
        """
        生成带 timestamp & sign 的 webhook URL
        NOTE: 测试用例可传 fixed now_ms 以获得确定性
        """
        timestamp = str(now_ms if now_ms is not None else round(time.time() * 1000))
        string_to_sign = f"{timestamp}\n{self.secret}"
        sign_raw = _hmac_sha256_b64(self.secret, string_to_sign)
        sign = urllib.parse.quote_plus(sign_raw)

        base = f"https://oapi.dingtalk.com/robot/send?access_token={self.access_token}"
        url = f"{base}&timestamp={timestamp}&sign={sign}"
        if self.extra_query:
            q = "&".join(f"{k}={urllib.parse.quote_plus(str(v))}" for k, v in self.extra_query.items())
            url = f"{url}&{q}"
        return url

    def _post(self, body: Dict, now_ms: Optional[int] = None) -> Dict:
        url = self._signed_url(now_ms=now_ms)
        resp = self.session.post(url, json=body, timeout=self.timeout,
                                 headers={"Content-Type": "application/json"})
        if resp.status_code != 200:
            raise DingRobotError(f"HTTP {resp.status_code}: {resp.text}")
        try:
            data = resp.json()
        except Exception as e:
            raise DingRobotError(f"Invalid JSON response: {resp.text}") from e
        if data.get("errcode") != 0:
            raise DingRobotError(f"DingTalk err: {data}")
        return data

    @staticmethod
    def _at_block(is_at_all: bool = False,
                  at_user_ids: Optional[List[str]] = None,
                  at_mobiles: Optional[List[str]] = None) -> Dict:
        return {
            "isAtAll": bool(is_at_all),
            "atUserIds": at_user_ids or [],
            "atMobiles": at_mobiles or []
        }

    @staticmethod
    def append_mentions_in_text(base_text: str,
                                at_user_ids: Optional[List[str]] = None,
                                at_mobiles: Optional[List[str]] = None) -> str:
        """
        工具：把 @userId 与 @手机号 附加到文本末尾（Markdown 或 Text）
        - 钉钉要求：要想真正 @ 到人，需要在文本中出现 @xxx
        - 你也可以自行在文本中手动放置 @xxx，本方法仅做兜底
        """
        suffixes: List[str] = []
        if at_user_ids:
            suffixes += [f"@{uid}" for uid in at_user_ids]
        if at_mobiles:
            suffixes += [f"@{m}" for m in at_mobiles]
        if not suffixes:
            return base_text
        joiner = "\n\n" if base_text and not base_text.endswith("\n") else ""
        return f"{base_text}{joiner}{' '.join(suffixes)}"

    # ========= 消息类型 =========
    # Text （支持 @）
    def send_text(self,
                  content: str,
                  is_at_all: bool = False,
                  at_user_ids: Optional[List[str]] = None,
                  at_mobiles: Optional[List[str]] = None,
                  now_ms: Optional[int] = None) -> Dict:
        # 按文档，content 中应包含 @ 对象；这里自动补充（不影响你手动写 @）
        content2 = self.append_mentions_in_text(content, at_user_ids, at_mobiles)
        body = {
            "msgtype": "text",
            "text": {"content": content2},
            "at": self._at_block(is_at_all, at_user_ids, at_mobiles)
        }
        return self._post(body, now_ms=now_ms)

    # Markdown （支持 @）
    def send_markdown(self,
                      title: str,
                      text_md: str,
                      is_at_all: bool = False,
                      at_user_ids: Optional[List[str]] = None,
                      at_mobiles: Optional[List[str]] = None,
                      now_ms: Optional[int] = None) -> Dict:
        text2 = self.append_mentions_in_text(text_md, at_user_ids, at_mobiles)
        body = {
            "msgtype": "markdown",
            "markdown": {"title": title, "text": text2},
            "at": self._at_block(is_at_all, at_user_ids, at_mobiles)
        }
        return self._post(body, now_ms=now_ms)

    # Link（不支持 @）
    def send_link(self,
                  title: str,
                  text: str,
                  message_url: str,
                  pic_url: Optional[str] = None,
                  now_ms: Optional[int] = None) -> Dict:
        body = {
            "msgtype": "link",
            "link": {
                "title": title,
                "text": text,
                "messageUrl": message_url,
                "picUrl": pic_url or ""
            }
        }
        return self._post(body, now_ms=now_ms)

    # ActionCard：整体跳转（支持 @，通过 text 中 @userId）
    def send_action_card_overall(self,
                                 title: str,
                                 text_md: str,
                                 single_title: str,
                                 single_url: str,
                                 btn_orientation: int = 0,
                                 hide_avatar: int = 0,
                                 # 规范上：如需 @，在 text_md 中自行加入 @userId；这里也可代插
                                 at_user_ids: Optional[List[str]] = None,
                                 at_mobiles: Optional[List[str]] = None,
                                 is_at_all: bool = False,
                                 now_ms: Optional[int] = None) -> Dict:
        text2 = self.append_mentions_in_text(text_md, at_user_ids, at_mobiles)
        body = {
            "msgtype": "actionCard",
            "actionCard": {
                "title": title,
                "text": text2,
                "btnOrientation": str(int(btn_orientation)),
                "hideAvatar": str(int(hide_avatar)),
                "singleTitle": single_title,
                "singleURL": single_url
            }
        }
        # 按文档 ActionCard 也接受 at 块
        body["at"] = self._at_block(is_at_all, at_user_ids, at_mobiles)
        return self._post(body, now_ms=now_ms)

    # ActionCard：独立跳转（支持 @，通过 text 中 @userId）
    def send_action_card_multi(self,
                               title: str,
                               text_md: str,
                               btns: List[Dict[str, str]],
                               btn_orientation: int = 0,
                               hide_avatar: int = 0,
                               at_user_ids: Optional[List[str]] = None,
                               at_mobiles: Optional[List[str]] = None,
                               is_at_all: bool = False,
                               now_ms: Optional[int] = None) -> Dict:
        if not btns:
            raise ValueError("btns 不能为空")
        # 校验按钮键名
        for i, b in enumerate(btns):
            if "title" not in b or "actionURL" not in b:
                raise ValueError(f"btns[{i}] 需包含 title 与 actionURL")
        text2 = self.append_mentions_in_text(text_md, at_user_ids, at_mobiles)
        body = {
            "msgtype": "actionCard",
            "actionCard": {
                "title": title,
                "text": text2,
                "btnOrientation": str(int(btn_orientation)),
                "hideAvatar": str(int(hide_avatar)),
                "btns": btns
            }
        }
        body["at"] = self._at_block(is_at_all, at_user_ids, at_mobiles)
        return self._post(body, now_ms=now_ms)

    # FeedCard（不支持 @）
    def send_feed_card(self,
                       items: List[Dict[str, str]],
                       now_ms: Optional[int] = None) -> Dict:
        """
        items: [{"title": "...", "messageURL": "...", "picURL": "..."}, ...]
        """
        if not items:
            raise ValueError("FeedCard items 不能为空")
        for i, it in enumerate(items):
            for k in ("title", "messageURL", "picURL"):
                if k not in it or not it[k]:
                    raise ValueError(f"items[{i}] 缺少 {k}")
        body = {"msgtype": "feedCard", "feedCard": {"links": items}}
        return self._post(body, now_ms=now_ms)


# ========= 可选：日志推送 =========
class DingTalkLogHandler(logging.Handler):
    """
    将 ERROR/CRITICAL 日志以 Markdown 推送到钉钉
    使用：
        robot = DingRobot(ACCESS_TOKEN, SECRET)
        h = DingTalkLogHandler(robot, level=logging.ERROR, app_name="algo-node")
        h.setFormatter(logging.Formatter("[%(asctime)s] %(name)s %(levelname)s: %(message)s"))
        logging.getLogger().addHandler(h)
    """
    def __init__(self, robot: DingRobot, level=logging.ERROR, app_name: str = "app"):
        super().__init__(level=level)
        self.robot = robot
        self.app_name = app_name

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            md = (
                f"### [{self.app_name}] {record.levelname}\n"
                f"- logger: `{record.name}`\n"
                f"- when: `{time.strftime('%Y-%m-%d %H:%M:%S')}`\n"
                f"- message:\n\n```\n{msg}\n```\n"
            )
            self.robot.send_markdown(title=f"{self.app_name} {record.levelname}", text_md=md)
        except Exception:
            # 避免日志处理再抛异常影响主流程
            pass
