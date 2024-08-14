#!/usr/bin/env python 3.11
# -*- coding:utf-8 -*-
# author:Dasin time:2023/8/21

# 内置库
import asyncio
from typing import Optional

# 安装的外部库
import httpx
import aiohttp
import requests


async def async_crawl(client: httpx.AsyncClient, url: str, display_error: bool = True
                      ) -> Optional[requests.Response]:
    """
    协程版: 访问的GET方式访问url，并返回req对象
    :param client: 传入的 httpx.AsyncClient
    :param url: 待爬取的 url
    :param display_error: 是否返回报错信息
    :return:
    """
    try:
        rep = await client.get(url, headers={'user-agent': 'Mozilla/5.0'}, timeout=500)
        rep.raise_for_status()
        return rep
    except asyncio.TimeoutError:
        return _handle_error("请求超时，请检查网络连接或尝试其他网址。", display_error)
    except aiohttp.ClientResponseError as e:
        return _handle_error(f"请求失败：{e}", display_error)
    except aiohttp.ClientError as e:
        return _handle_error(f"请求异常：{e}", display_error)
    except Exception as e:
        # 捕获未知异常，处理其他异常情况(可能是由于网络连接中断、服务器错误或其他连接问题引起的)
        return _handle_error(f"未知异常：{e}", display_error)


def standard_crawl(url, display_error: bool = True) -> Optional[requests.Response]:
    """
    单线程版本： 访问的GET方式访问url，并返回req对象
    :param url: 待爬取的 url
    :param display_error: 是否返回报错信息
    :return:
    """
    try:
        req = requests.get(url, headers={'user-agent': 'Mozilla/5.0'}, timeout=500)
        req.raise_for_status()
        return req
    except requests.exceptions.Timeout:
        return _handle_error("请求超时，请检查网络连接或尝试其他网址。", display_error)
    except requests.exceptions.HTTPError as e:
        return _handle_error(f"请求失败：{e}", display_error)
    except requests.exceptions.RequestException as e:
        return _handle_error(f"请求异常：{e}", display_error)
    except Exception as e:
        return _handle_error(f"未知异常：{e}", display_error)


def _handle_error(message: str, display_error: bool = True) -> None:
    """
    处理错误信息的辅助函数
    :param message: 错误信息
    :param display_error: 是否输出错误信息
    :return: None
    """
    if display_error:
        print(message)
    return None
