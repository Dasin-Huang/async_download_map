#!/usr/bin/env python 3
# -*- coding: utf-8 -*-
"""
Description: linux 停止进程的命令  pkill -f main.py 查询进程的命令 ps aux | grep <process_name>
挂起运行代码 nohup python main.py  nohup python main.py > /dev/null 2>&1 &
File: main.py
Author: dasin
Time: 2024/1/25 20:04
"""

from time import perf_counter, gmtime, strftime
from typing import Tuple

from async_download_map import MapTilesDownloader


def main():
    # 创建多进程驱动下载
    styles = ['DarkMatter', 'MapTilerBasic', 'OSMBridght', 'OSMLiberty', 'Positron']
    # styles = ['DarkMatter']
    for style in styles:
        downloader = my_download(style, (10, 14), False, False)
        downloader.download_map_tiles()


def my_download(style: str, zoom_range: Tuple[int, int], if_local: bool = True, if_log: bool = True
                ) -> MapTilesDownloader:
    """
    根据风格完成瓦片的爬取
    :param style: 下载的风格
    :param zoom_range: 下载的缩放等级
    :param if_local: 是否为本地爬取
    :param if_log: 是否记录文档
    :return:
    """
    logfile = f'{style}_log.txt' if if_log else None
    if if_local:
        # 本地爬取版本
        return MapTilesDownloader(f'http://localhost:8080/styles/{style}',
                                  (2513206.19, 12757547.61), (2713612.06, 12462217),
                                  zoom_range, f'{style}.gz', logfile)
    else:
        # 在线爬取版本
        return MapTilesDownloader(f'http://192.168.60.234:8080/styles/{style}',
                                  (2513206.19, 12757547.61), (2713612.06, 12462217),
                                  zoom_range, f'{style}.gz', logfile)


if __name__ == '__main__':
    t_start = perf_counter()
    # 运行主程序
    main()
    print(f'程序运行结束，总耗时{strftime("%H:%M:%S", gmtime(perf_counter() - t_start))}')
