#!/usr/bin/env python 3
# -*- coding: utf-8 -*-
"""
Description: 
File: map_tiles_downloader
Author: Dasin
Time: 2024-08- 15:02
"""

import os
import sys
import asyncio
import zipfile
import threading
import math as m
from time import time, sleep, gmtime, strftime
from typing import List, Tuple, Literal, Optional

# 需要安装的库
import httpx
import requests
from tqdm import tqdm

from .spider_tolls import async_crawl, standard_crawl


class MapTilesDownloader:
    # 定义常量
    EARTH_RADIUS = 20037508.3427892
    # 创建一个全局锁对象
    zip_file_lock = threading.Lock()

    def __init__(self, base_url: str, top_left_coord: Tuple[float, float], bottom_right_coord: Tuple[float, float],
                 zoom_range: Tuple[int, int], save_file: str, log_file: str = None):
        """
        初始化瓦片爬取函数
        :param base_url: 瓦片爬取基础的 url (不含最有的 '/')
        :param top_left_coord: 待爬取的矩形范围的左上角的坐标 (x, y)
        :param bottom_right_coord: 待爬取的矩形范围的右下角的坐标 (x, y)
        :param zoom_range: 缩放等级的下载范围
        :param save_file: 保存的文件夹
        :param log_file: 文件日志，如不输出到日志则为空
        """
        # 对点坐标进行解构
        tl_lon, tl_lat = top_left_coord
        br_lon, br_lat = bottom_right_coord
        # 进行范围判断
        if tl_lon >= br_lon or tl_lat <= br_lat:
            raise ValueError('请输入正确的爬取范围，第一个点为左上角坐标，第二个点为右下角坐标')
        # 初始化变量
        self.base_url = base_url
        self.tl_lon = tl_lon
        self.tl_lat = tl_lat
        self.br_lon = br_lon
        self.br_lat = br_lat
        self.zoom_range = zoom_range
        self.save_file = save_file
        self.logfile = log_file

        # 下载过程中需要创建的中间
        self.zip_file = None
        self.urls = None
        self.filenames = None

    def __str__(self):
        """返回对象信息"""
        test_url = self.generate_tile_urls(self.zoom_range[0])[0][0]
        return (f'爬取范围：{(self.tl_lon, self.tl_lat)}~{(self.br_lon, self.br_lat)}\n，'
                f'缩放等级：{self.zoom_range}，存储位置：{self.save_file}\n'
                f'样例连接为：{test_url}')

    def __repr__(self):
        """查看类"""
        return (f'MapTilesDownloader(base_url={self.base_url}, '
                f'top_left_coord={(self.tl_lon, self.tl_lat)}, '
                f'bottom_right_coord={(self.br_lon, self.br_lat)},'
                f'zoom_range={self.zoom_range}, '
                f'save_file={self.save_file}, '
                f'logfile={self.logfile})')

    def download_map_tiles(self):
        # 重定向输出
        if self.logfile is not None:
            sys.stdout = open(self.logfile, 'w')
        # 创建存储的文件夹
        if save_path := os.path.dirname(self.save_file):
            os.makedirs(save_path, exist_ok=True)
        # 创建存储的压缩包
        self.zip_file = zipfile.ZipFile(self.save_file, 'w')
        # 遍历缩放等级，进行下载
        for z in range(*self.zoom_range):
            t1 = time()
            # 刷新 log 文档
            sys.stdout.flush()

            # 生成待爬取的 url
            self.urls, self.filenames = self.generate_tile_urls(z)
            # 遍历 url_list 下载图像
            self.run_tile_downloads('download')
            # 检查图像的完整性
            self.verify_tile_integrity()

            total = len(self.urls)
            duration = time() - t1
            print(f'等级为{z:>2}的瓦片下载完成，共{total}张，'
                  f'下载速度为{int(total / duration)}张/s，耗时{self.seconds_to_hms(duration)}！')
        # 显式关闭压缩包
        self.zip_file.close()

    def generate_tile_urls(self, zoom: int) -> Tuple[List[str], List[str]]:
        """
        生成待爬取的 url 列表
        :param zoom: 缩放等级
        :return:
        """
        # 获取图片的编号
        min_row, max_row, min_col, max_col = self.calculate_tile_indices(zoom)
        url_list = [f'{self.base_url}/{zoom}/{i}/{j}.png'
                    for i in range(min_row, max_row + 1) for j in range(min_col, max_col + 1)]
        filename_list = [f'{zoom}/{i}/{j}.png'
                         for i in range(min_row, max_row + 1) for j in range(min_col, max_col + 1)]
        return url_list, filename_list

    def calculate_tile_indices(self, zoom: int) -> Tuple[int, int, int, int]:
        """
        根据区域的范围，获取指定缩放等级下待爬取范围矩形的瓦片索引（左上角和右下角的坐标），坐标系为 Web墨卡托（epsg=3857）
        :param zoom:
        :return:
        """
        # 生成行列号
        radius_factor = 2 ** zoom / (self.EARTH_RADIUS * 2)
        max_col = m.floor(((self.EARTH_RADIUS - self.tl_lon) * radius_factor)) + 2
        min_col = m.floor(((self.EARTH_RADIUS - self.br_lon) * radius_factor)) - 2
        max_row = m.floor(((self.EARTH_RADIUS + self.tl_lat) * radius_factor)) + 2
        min_row = m.floor(((self.EARTH_RADIUS + self.br_lat) * radius_factor)) - 2
        return min_row, max_row, min_col, max_col

    def run_tile_downloads(self, mode: Literal['download', 'filling']) -> None:
        """驱动协程下载图片"""
        if self.urls is not None:
            # 创建进度条的图例
            style_name = self.urls[0].split("/")[4]
            zoom = self.urls[0].split("/")[5]
            if mode == 'download':
                task_name = f'{style_name} 瓦片(zoom={zoom:>2})下载'
                semaphore = 100
            else:
                task_name = f'{style_name} 瓦片(zoom={zoom:>2})补齐'
                semaphore = 30
            # 协程驱动下载
            asyncio.run(self.fetch_multiple_tiles_async(task_name, semaphore))
        else:
            raise IndexError('无要下载的url，请核实代码')

    async def fetch_multiple_tiles_async(self, task_name: str, semaphore: int) -> None:
        """
        协程下载图片
        :param task_name: 当前主任务的名称
        :param semaphore: 协程并发量
        :return:
        """
        # 限制最大并发量
        semaphore = asyncio.Semaphore(semaphore)

        async def sem_download(*args):
            async with semaphore:
                return await self.fetch_single_tile_async(*args)

        async with httpx.AsyncClient() as client:
            # 创建协程任务
            tasks = [sem_download(client, url, filename, False) for url, filename in
                     zip(self.urls, self.filenames)]
            # 使用进度条驱动协程运行
            to_do_iter = tqdm(asyncio.as_completed(tasks), total=len(self.urls), desc=task_name, dynamic_ncols=True,
                              colour='#729c1f', leave=None, position=0)
            # 运行协程
            for coro in to_do_iter:
                await coro

    def fetch_single_tile(self, url: str, filename: str, display_error: bool = True) -> None:
        """
        获取单张并保存图片
        :param url: 瓦片的 url 链接
        :param filename: 保存的文件名
        :param display_error: 是否显示错误信息
        :return: None
        """
        # 获取图片
        resp = standard_crawl(url)
        # 如果图片返回正确，则保存图片
        if resp is not None and resp.headers.get('Content-Type', '').startswith('image'):
            try:
                # 存储文件
                self.zip_file.writestr(filename, resp.content)
            except Exception as e:
                # 记录错误并继续进行后续的下载
                if display_error:
                    print(f'保存图片时出错: {e} - {url}')
        else:
            # 记录错误并继续进行后续的下载
            print(f'请求出错或返回的结果不是图片: {url}')

    async def fetch_single_tile_async(self, client: httpx.AsyncClient, url: str, filename: str,
                                      display_error: bool = True) -> None:
        """
        协程下载一个图片
        :param client: 异步的客户端
        :param url: 瓦片的 url 链接
        :param filename: 保存的文件名
        :param display_error: 是否显示错误信息
        :return: None
        """
        resp = await async_crawl(client, url, False)
        if resp is not None and resp.headers.get('Content-Type', '').startswith('image'):
            try:
                # 开辟线程写入文件
                await asyncio.to_thread(self.save_image, resp, filename)
            except Exception as e:
                if display_error:
                    print(f'保存图片时出错: {e} - {url}')
        else:
            if display_error:
                print(f'请求出错或返回的结果不是图片: {url}')

    def verify_tile_integrity(self) -> None:
        """
        验证瓦片文件的完整性
        :return:
        """
        max_attempts = 5  # 设置最大尝试次数
        attempts = 0
        while attempts < max_attempts:
            tiles_result = self.zip_file.namelist()
            if set(self.filenames).issubset(set(tiles_result)):
                # 补齐所有瓦片，结束循环
                break
            # 收集缺失的瓦片
            tiles_missing = set(self.filenames).difference(set(tiles_result))

            if attempts != 0:
                # 延时一段时间再进行下一次尝试，避免过于频繁的请求
                sleep(2)

            if attempts == 0:
                # 协程尝试补齐一次
                missing_url_list = [self.urls[self.filenames.index(tile_name)] for tile_name in tiles_missing]
                self.run_tile_downloads('filling')
            else:
                # 逐张下载
                for tile_name in tiles_missing:
                    url = self.urls[self.filenames.index(tile_name)]
                    self.fetch_single_tile(url, tile_name, False)
            # 尝试次数 + 1
            attempts += 1

        if attempts >= max_attempts:
            # 多次尝试后仍未补充完整，抛出异常或结束程序
            raise Exception("多次尝试后无法补齐所有瓦片，请检查网络连接或其他问题。")

    def save_image(self, resp: requests.Response, filename: str) -> Optional[Exception]:
        """
        保存图片(线程安全)
        :param resp:
        :param filename: 
        :return: 
        """
        try:
            with self.zip_file_lock:
                self.zip_file.writestr(filename, resp.content)
        except Exception as e:
            return e
        return None

    @staticmethod
    def seconds_to_hms(second: float | int) -> str:
        """将秒数转换为时分秒"""
        return strftime("%H:%M:%S", gmtime(second))
